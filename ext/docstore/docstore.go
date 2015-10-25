/*

Package docstore implements a JSON-based document store
built on top of the Versioned Key-Value store and the Blob store.

Each document will get assigned a MongoDB like ObjectId encoded as hex:

	<binary encoded uint32 (4 bytes) + blob ref (32 bytes)>

The JSON document will be stored as is and kvk entry will reference it.

	docstore:<collection>:<id> => (empty)

The pointer contains an empty value since the hash is contained in the id.

*/
package docstore
