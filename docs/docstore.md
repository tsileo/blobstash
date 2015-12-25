# Document Store Extension

A JSON document store running on top of an HTTP API. Support a subset of the MongoDB Query language.

JSON documents are stored as blobs and the key-value store handle the indexing.

Perfect for building app designed to only store your own data.

### Supported MongoDB query operators

Refers to MongpDB documentation: [query documents](https://docs.mongodb.org/manual/tutorial/query-documents/) and [query operators](https://docs.mongodb.org/manual/reference/operator/query/#query-selectors).

#### Features

- [x] dot-notation support
- [x] `{}` - Select all documents
- [x] `{ <field>: <value> }` - equality, AND conditions

#### Query operators

##### Comparison

- [x] `$eq`
- [x] `$gt`
- [x] `$gte`
- [x] `$lt`
- [x] `$lte`
- [ ] `$ne`
- [ ] `$in`
- [ ] `$nin`

##### Logical

- [x] `$or`
- [x] `$and`
- [ ] `$not`
- [ ] `$nor`

##### Element

- [ ] `$exists`
- [ ] `$type`

##### Evalutation

- [ ] `$regex`


