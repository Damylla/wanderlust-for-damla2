import Foundation
import Combine
import Network

#if canImport(FirebaseFirestore)
import FirebaseFirestore
#endif

enum SyncState: Equatable {
    case idle
    case syncing
    case synced(Date)
    case offline
    case failed(String)
    case conflict
}

enum SyncEntityType: String, Codable, Sendable {
    case profile
    case trip
    case tripGroup
    case flight
    case settings
    case tombstone
}

enum SyncOperationType: String, Codable, Sendable {
    case create
    case update
    case delete
}

enum SyncQueueItemState: String, Codable, Sendable {
    case pending
    case inFlight
    case failed
    case completed
}

struct SyncQueueOperation: Identifiable, Codable, Hashable, Sendable {
    let operationId: UUID
    var entityType: SyncEntityType
    var entityId: String
    var operationType: SyncOperationType
    var payload: Data?
    var createdAt: Date
    var retryCount: Int
    var lastError: String?
    var state: SyncQueueItemState

    var id: UUID { operationId }

    init(
        operationId: UUID = UUID(),
        entityType: SyncEntityType,
        entityId: String,
        operationType: SyncOperationType,
        payload: Data? = nil,
        createdAt: Date = Date(),
        retryCount: Int = 0,
        lastError: String? = nil,
        state: SyncQueueItemState = .pending
    ) {
        self.operationId = operationId
        self.entityType = entityType
        self.entityId = entityId
        self.operationType = operationType
        self.payload = payload
        self.createdAt = createdAt
        self.retryCount = retryCount
        self.lastError = lastError
        self.state = state
    }
}

@MainActor
final class SyncQueueStore: ObservableObject {
    static let shared = SyncQueueStore()

    @Published private(set) var operations: [SyncQueueOperation] = []
    @Published private(set) var syncState: SyncState = .idle
    @Published private(set) var isOnline = true

    private let userDefaults: UserDefaults
    private let storageKey = "atlas.syncQueue.operations"
    private var activeUID: String?
    private let monitor = NWPathMonitor()
    private let monitorQueue = DispatchQueue(label: "atlas.sync.network")
    private var isProcessing = false
    private var storageGeneration = 0
    private var activeProcessingHandler: ((SyncQueueOperation) async throws -> Void)?
    private var queuedProcessingHandler: ((SyncQueueOperation) async throws -> Void)?
    private var shouldProcessAgain = false
    private var onSyncRequested: (() -> Void)?
    private let maxRetryCount = 6

    init(userDefaults: UserDefaults = .standard) {
        self.userDefaults = userDefaults
        load()
        startNetworkMonitoring()
    }

    func enqueue(
        entityType: SyncEntityType,
        entityId: String,
        operationType: SyncOperationType,
        payload: Data? = nil
    ) {
        let normalizedID = entityId.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !normalizedID.isEmpty else { return }

        operations.removeAll {
            $0.entityType == entityType &&
            $0.entityId == normalizedID &&
            $0.operationType != .delete &&
            operationType == .delete
        }
        operations.removeAll {
            $0.entityType == entityType &&
            $0.entityId == normalizedID &&
            $0.operationType == operationType &&
            $0.state != .completed
        }

        operations.append(
            SyncQueueOperation(
                entityType: entityType,
                entityId: normalizedID,
                operationType: operationType,
                payload: payload
            )
        )
        persist()
        updateIdleState()
        if isProcessing, let activeProcessingHandler {
            queuedProcessingHandler = activeProcessingHandler
            shouldProcessAgain = true
        }
    }

    func activate(for uid: String) {
        storageGeneration += 1
        activeUID = uid
        load()
    }

    func deactivate() {
        storageGeneration += 1
        activeUID = nil
        load()
    }

    func setSyncRequestHandler(_ handler: @escaping () -> Void) {
        onSyncRequested = handler
    }

    func enqueue<T: Encodable>(
        entityType: SyncEntityType,
        entityId: String,
        operationType: SyncOperationType,
        payload: T,
        encoder: JSONEncoder = JSONEncoder()
    ) {
        do {
            let data = try encoder.encode(payload)
            enqueue(
                entityType: entityType,
                entityId: entityId,
                operationType: operationType,
                payload: data
            )
        } catch {
            syncState = .failed(sanitizedErrorMessage(error))
            WanderlustDiagnostics.error(error, category: .sync, message: "sync queue payload encoding failed")
        }
    }

    func processPendingOperations(
        handler: @escaping (SyncQueueOperation) async throws -> Void
    ) async {
        if isProcessing {
            queuedProcessingHandler = handler
            shouldProcessAgain = true
            return
        }
        guard isOnline else {
            syncState = .offline
            return
        }

        isProcessing = true
        let processingUID = activeUID
        let generation = storageGeneration
        activeProcessingHandler = handler
        let operationIDs = operations.map(\.operationId)
        syncState = .syncing
        defer {
            isProcessing = false
            activeProcessingHandler = nil
            updateIdleState()
            if shouldProcessAgain, let queuedProcessingHandler {
                shouldProcessAgain = false
                self.queuedProcessingHandler = nil
                Task { @MainActor [weak self] in
                    await self?.processPendingOperations(handler: queuedProcessingHandler)
                }
            }
        }

        for operationID in operationIDs {
            guard activeUID == processingUID,
                  storageGeneration == generation,
                  let index = operations.firstIndex(where: { $0.operationId == operationID }) else {
                if activeUID != processingUID || storageGeneration != generation { return }
                continue
            }
            var operation = operations[index]
            guard operation.state != .completed else { continue }
            guard operation.retryCount < maxRetryCount else { continue }

            operations[index].state = .inFlight
            persist()

            do {
                try await handler(operation)
                guard activeUID == processingUID,
                      storageGeneration == generation,
                      let currentIndex = operations.firstIndex(where: { $0.operationId == operationID }) else {
                    if activeUID != processingUID || storageGeneration != generation { return }
                    continue
                }
                operations[currentIndex].state = .completed
                operations[currentIndex].lastError = nil
            } catch {
                guard activeUID == processingUID,
                      storageGeneration == generation,
                      let currentIndex = operations.firstIndex(where: { $0.operationId == operationID }) else {
                    if activeUID != processingUID || storageGeneration != generation { return }
                    continue
                }
                operation.retryCount += 1
                operation.lastError = sanitizedErrorMessage(error)
                operation.state = .failed
                operations[currentIndex] = operation
                WanderlustDiagnostics.error(
                    error,
                    category: .sync,
                    message: "sync queue operation failed",
                    metadata: [
                        "entityType": operation.entityType.rawValue,
                        "entityId": operation.entityId,
                        "operationType": operation.operationType.rawValue
                    ]
                )
            }
            persist()
        }

        operations.removeAll { $0.state == .completed }
        persist()
    }

    func retryFailed() {
        var didRetry = false
        for index in operations.indices where operations[index].state == .failed {
            operations[index].state = .pending
            didRetry = true
        }
        persist()
        updateIdleState()
        if didRetry {
            onSyncRequested?()
        }
    }

    func removeOperations(entityType: SyncEntityType) {
        operations.removeAll { $0.entityType == entityType }
        persist()
        updateIdleState()
    }

    private func load() {
        guard let data = userDefaults.data(forKey: currentStorageKey) else {
            operations = []
            updateIdleState()
            return
        }
        do {
            operations = try JSONDecoder().decode([SyncQueueOperation].self, from: data)
        } catch {
            operations = []
            WanderlustDiagnostics.error(error, category: .migration, message: "sync queue decode failed")
        }
        updateIdleState()
    }

    private func persist() {
        do {
            let data = try JSONEncoder().encode(operations)
            userDefaults.set(data, forKey: currentStorageKey)
        } catch {
            WanderlustDiagnostics.error(error, category: .storage, message: "sync queue persist failed")
        }
    }

    private func updateIdleState() {
        if !isOnline {
            syncState = .offline
        } else if operations.contains(where: { $0.state == .failed }) {
            syncState = .failed(WanderlustL10n.string("Sync failed. Try again.", table: .errors, fallback: "Sync failed. Try again."))
        } else if operations.contains(where: { $0.state == .pending || $0.state == .inFlight }) {
            syncState = .idle
        } else {
            syncState = .synced(Date())
        }
    }

    private var currentStorageKey: String {
        guard let activeUID, !activeUID.isEmpty else { return storageKey }
        let safeUID = activeUID
            .replacingOccurrences(of: "/", with: "_")
            .replacingOccurrences(of: ":", with: "_")
        return "\(storageKey).\(safeUID)"
    }

    private func startNetworkMonitoring() {
        monitor.pathUpdateHandler = { [weak self] path in
            let online = path.status == .satisfied
            Task { @MainActor [weak self, online] in
                guard let self else { return }
                let wasOnline = self.isOnline
                self.isOnline = online
                self.updateIdleState()
                if online && !wasOnline {
                    self.onSyncRequested?()
                }
            }
        }
        monitor.start(queue: monitorQueue)
    }

    private func sanitizedErrorMessage(_ error: Error) -> String {
        let message = error.localizedDescription
        return message.count > 240 ? String(message.prefix(240)) : message
    }
}

@MainActor
enum WanderlustConflictResolver {
    struct MergeSummary: Equatable {
        var addedTripCount = 0
        var mergedTripCount = 0
        var unchangedTripCount = 0
        var failedTripCount = 0
    }

    struct MergeOutput {
        var trips: [Trip]
        var summary: MergeSummary
    }

    static func mergedTrips(localTrips: [Trip], remoteTrips: [Trip], deletedTripIDs: Set<UUID> = []) -> [Trip] {
        mergeGuestTrips(guestTrips: localTrips, accountTrips: remoteTrips, deletedTripIDs: deletedTripIDs).trips
    }

    static func mergeGuestTrips(guestTrips: [Trip], accountTrips: [Trip], deletedTripIDs: Set<UUID> = []) -> MergeOutput {
        var summary = MergeSummary()
        var merged = Dictionary(uniqueKeysWithValues: accountTrips.map { ($0.id, $0) })
        var duplicateKeyIndex = Dictionary(uniqueKeysWithValues: accountTrips.map { (tripDuplicateKey($0), $0.id) })

        for guest in guestTrips {
            if deletedTripIDs.contains(guest.id) {
                merged.removeValue(forKey: guest.id)
                duplicateKeyIndex.removeValue(forKey: tripDuplicateKey(guest))
                WanderlustDiagnostics.info("delete wins conflict entityType=trip entityId=\(guest.id.uuidString)", category: .sync)
                continue
            }

            if let account = merged[guest.id] {
                let resolved = mergeTrip(local: guest, remote: account)
                if resolved == account {
                    summary.unchangedTripCount += 1
                } else {
                    summary.mergedTripCount += 1
                }
                merged[guest.id] = resolved
                duplicateKeyIndex[tripDuplicateKey(resolved)] = resolved.id
                continue
            }

            let duplicateKey = tripDuplicateKey(guest)
            if let existingID = duplicateKeyIndex[duplicateKey],
               let account = merged[existingID] {
                let resolved = mergeTrip(local: guest, remote: account)
                merged[existingID] = resolved
                duplicateKeyIndex[duplicateKey] = existingID
                summary.mergedTripCount += 1
            } else {
                merged[guest.id] = guest
                duplicateKeyIndex[duplicateKey] = guest.id
                summary.addedTripCount += 1
            }
        }

        return MergeOutput(trips: Array(merged.values), summary: summary)
    }

    static func mergedTripsByID(localTrips: [Trip], remoteTrips: [Trip], deletedTripIDs: Set<UUID> = []) -> [Trip] {
        var merged = Dictionary(uniqueKeysWithValues: remoteTrips.map { ($0.id, $0) })
        for local in localTrips {
            if deletedTripIDs.contains(local.id) {
                merged.removeValue(forKey: local.id)
                WanderlustDiagnostics.info("delete wins conflict entityType=trip entityId=\(local.id.uuidString)", category: .sync)
                continue
            }

            guard let remote = merged[local.id] else {
                merged[local.id] = local
                continue
            }

            merged[local.id] = mergeTrip(local: local, remote: remote)
        }

        for remote in remoteTrips {
            if deletedTripIDs.contains(remote.id) {
                merged.removeValue(forKey: remote.id)
                WanderlustDiagnostics.info("delete wins conflict entityType=trip entityId=\(remote.id.uuidString)", category: .sync)
                continue
            }

            guard let local = merged[remote.id] else {
                merged[remote.id] = remote
                continue
            }

            merged[remote.id] = mergeTrip(local: local, remote: remote)
        }
        return Array(merged.values)
    }

    static func mergeTrip(local: Trip, remote: Trip) -> Trip {
        let preferLocal = local.updatedAt >= remote.updatedAt
        var result = preferLocal ? local : remote
        let fallback = preferLocal ? remote : local

        result.title = preferredString(result.title, fallback.title)
        result.notes = mergedNotes(primary: result.notes, secondary: fallback.notes)
        result.startDate = min(local.startDate, remote.startDate)
        result.endDate = max(local.endDate, remote.endDate)
        result.createdAt = min(local.createdAt, remote.createdAt)
        result.updatedAt = max(local.updatedAt, remote.updatedAt)
        result.isFavorite = local.isFavorite || remote.isFavorite
        result.transportationTypes = Array(Set(local.transportationTypes).union(remote.transportationTypes))
            .sorted { $0.sortOrder < $1.sortOrder }
        result.cities = mergeCities(local.cities, remote.cities)
        result.flights = mergeFlights(local.flights, remote.flights)
        return result
    }

    private static func mergeByID<T: Identifiable>(_ local: [T], _ remote: [T], resolve: (T, T) -> T) -> [T] where T.ID: Hashable {
        var merged = Dictionary(uniqueKeysWithValues: local.map { ($0.id, $0) })
        for item in remote {
            if let existing = merged[item.id] {
                merged[item.id] = resolve(existing, item)
            } else {
                merged[item.id] = item
            }
        }
        return Array(merged.values)
    }

    private static func mergeCities(_ first: [VisitedCity], _ second: [VisitedCity]) -> [VisitedCity] {
        var merged: [String: VisitedCity] = [:]
        for city in first + second {
            let key = cityKey(city)
            if var existing = merged[key] {
                existing.periods = mergeByID(existing.periods, city.periods) { left, right in
                    left.endDate >= right.endDate ? left : right
                }.sorted { $0.startDate < $1.startDate }
                existing.notes = mergedNotes(primary: existing.notes, secondary: city.notes)
                merged[key] = existing
            } else {
                merged[key] = city
            }
        }
        return Array(merged.values).sortedByVisitDate()
    }

    private static func mergeFlights(_ first: [Flight], _ second: [Flight]) -> [Flight] {
        var merged: [String: Flight] = [:]
        for flight in first + second {
            let key = flightKey(flight)
            if let existing = merged[key] {
                merged[key] = flight.departureDate >= existing.departureDate ? flight : existing
            } else {
                merged[key] = flight
            }
        }
        return Array(merged.values).normalizedFlightList()
    }

    private static func tripDuplicateKey(_ trip: Trip) -> String {
        let countryCodes = trip.cities
            .map { normalizedCountryCode($0.countryCode, fallback: $0.countryName) }
            .filter { !$0.isEmpty }
            .sorted()
            .joined(separator: ",")
        let cityIDs = trip.cities.map(cityKey).sorted().joined(separator: ",")
        return [
            countryCodes,
            cityIDs,
            dayKey(trip.startDate),
            dayKey(trip.endDate)
        ].joined(separator: "|")
    }

    private static func cityKey(_ city: VisitedCity) -> String {
        if let geonameID = city.geonameID {
            return "geoname:\(geonameID)"
        }
        return [
            normalize(city.name),
            normalizedCountryCode(city.countryCode, fallback: city.countryName),
            coordinateKey(city.latitude),
            coordinateKey(city.longitude)
        ].joined(separator: "|")
    }

    private static func flightKey(_ flight: Flight) -> String {
        let legs = flight.effectiveLegs.map { leg in
            [
                normalize(leg.departureAirportID.isEmpty ? leg.departureAirportCode : leg.departureAirportID),
                normalize(leg.arrivalAirportID.isEmpty ? leg.arrivalAirportCode : leg.arrivalAirportID),
                normalize(leg.airlineCode),
                normalize(leg.flightNumber),
                minuteKey(leg.departureDate)
            ].joined(separator: ":")
        }.joined(separator: "|")

        if !legs.isEmpty {
            return legs
        }

        return [
            normalize(flight.departureAirportID.isEmpty ? flight.departureAirportCode : flight.departureAirportID),
            normalize(flight.arrivalAirportID.isEmpty ? flight.arrivalAirportCode : flight.arrivalAirportID),
            normalize(flight.airlineCode),
            normalize(flight.flightNumber),
            minuteKey(flight.departureDate)
        ].joined(separator: ":")
    }

    private static func preferredString(_ primary: String, _ fallback: String) -> String {
        let cleaned = primary.trimmingCharacters(in: .whitespacesAndNewlines)
        return cleaned.isEmpty ? fallback : primary
    }

    private static func mergedNotes(primary: String, secondary: String) -> String {
        let first = primary.trimmingCharacters(in: .whitespacesAndNewlines)
        let second = secondary.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !first.isEmpty else { return secondary }
        guard !second.isEmpty else { return primary }
        guard normalize(first) != normalize(second) else { return primary }
        return "\(primary)\n\n\(secondary)"
    }

    private static func normalizedCountryCode(_ code: String, fallback: String) -> String {
        let resolved = WanderlustNameResolver.normalizedCountryCode(code, fallback: fallback)
        return resolved.trimmingCharacters(in: .whitespacesAndNewlines).uppercased()
    }

    private static func dayKey(_ date: Date) -> String {
        let components = Calendar(identifier: .gregorian).dateComponents(in: TimeZone(secondsFromGMT: 0)!, from: date)
        return String(format: "%04d-%02d-%02d", components.year ?? 0, components.month ?? 0, components.day ?? 0)
    }

    private static func minuteKey(_ date: Date) -> String {
        String(Int(date.timeIntervalSince1970 / 60.0))
    }

    private static func coordinateKey(_ value: Double) -> String {
        String(format: "%.4f", value)
    }

    private static func normalize(_ text: String) -> String {
        text.trimmingCharacters(in: .whitespacesAndNewlines)
            .folding(options: [.caseInsensitive, .diacriticInsensitive, .widthInsensitive], locale: Locale(identifier: "en_US_POSIX"))
            .lowercased()
    }
}
