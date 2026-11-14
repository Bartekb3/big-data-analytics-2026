// Initialises the single-node replica set rs0 and creates the indexes from
// architecture/contracts/mongodb-collections.md (that file is the source of truth: change it first).
// Run by mongo-init with mongosh. Safe to run again: an existing replica set and identical indexes are kept;
// an existing index with other options than below fails the run (IndexOptionsConflict), on purpose.

try {
  let initiated = true;
  try {
    rs.status();
  } catch (e) {
    if (e.codeName !== "NotYetInitialized") throw e;
    initiated = false;
  }
  if (!initiated) {
    print("initiating replica set rs0");
    // Host as other containers reach it; from this computer use directConnection=true (see .env.example).
    rs.initiate({ _id: "rs0", members: [{ _id: 0, host: "mongodb:27017" }] });
  }

  const deadline = Date.now() + 60 * 1000;
  while (!db.hello().isWritablePrimary) {
    if (Date.now() > deadline) throw new Error("no PRIMARY after 60 s");
    sleep(1000);
  }
  print("replica set rs0 is PRIMARY");

  const g = db.getSiblingDB("gametrend");

  g.game_metrics.createIndex({ platform: 1, id: 1, ts: 1 }, { unique: true });
  g.game_metrics.createIndex({ platform: 1, ts: -1 });

  g.game_metrics_hourly.createIndex({ platform: 1, id: 1, hour_ts: 1 }, { unique: true });

  g.games.createIndex({ platform: 1, id: 1 }, { unique: true });
  g.games.createIndex({ platform: 1, genre_l1: 1 });

  g.tracked_games.createIndex({ platform: 1, id: 1 }, { unique: true });

  g.bv_matches.createIndex({ steam_id: 1, roblox_id: 1 }, { unique: true });
  g.bv_matches.createIndex({ roblox_id: 1 });

  g.model_coeffs.createIndex({ model: 1, platform: 1, trained_at: -1 }, { unique: true });

  g.model_health.createIndex({ platform: 1, evaluated_at: -1 }, { unique: true });

  g.ingest_stats.createIndex({ feed: 1, ts: 1 }, { unique: true });

  g.getCollectionNames().sort().forEach((c) => {
    print(c + ": " + g[c].getIndexes().map((i) => i.name).join(", "));
  });
} catch (e) {
  print("ERROR: " + e);
  quit(1);
}
