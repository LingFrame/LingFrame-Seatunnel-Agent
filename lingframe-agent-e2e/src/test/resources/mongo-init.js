const testDb = db.getSiblingDB('test');
testDb.events.insertMany(Array.from({length: 100}, (_, i) => ({id: NumberInt(i), name: 'event-' + i})));
