# ALS Fork of the AA
The goal of this fork is to provide support for a management workflow when working with the Archiver Appliance.

The appliance currently stores the channel meta data in a JSON string as a text blob when working with MariaDB. We'd prefer to have
a PVTypeInfo table and ArchivePVRequest table, where all the key pairs are broken out into columns. The exception is `extraFields` which is not defined enough
to follow this idea.

## Example Table Layout
For the time being, I've provided a script, `create_new_SQL_tables.sql` which contaings the updated schemas.


## Benefits and Workflow
By exposing the meta data directly in SQL, we can avoid writing apps which must first hit an AA servlet and decode a JSON. Wrapping API's and decoding JSON's
is rarely as short of code as you'd want and there's a lot of room for errors.

With the PVTypeInfo exposed such, it is much easier to write apps which can expect a well defined schema and since this schema is not subject to change, we expect to
lean on the relational nature of MariaDB.

The ArchivePVRequest table will have an `applianceId` columns which can hold contraints so that the correct appliances are used for various purposes for which they are designed. We wouldn't want
a scientitst adding a channel that archives ar 10 Hz and a subnet reserved for embedded systems, to be on any Appliance. Mistakes are made and the results can be difficult to untangle if they are even detected.

## Missing Channels and other Error Statess
Often times at the ALS, channels have been added to the AA during a maintance period. This is also when nework connections maybe down for several reasons. This leads to channels that do not make it itout of the "initial samping" or channels that are not found. There are many ways in which a channel may be submitted to the archiver and never actually becomes archived.

Instead of writing clients which decode JSON and use cumbersome logic, we prefer to add additional columns which store the state of channels. One idea, is to use a trigger on the PVInfoType table to create a row for every pvName in the ArchivePVRequest table, so simple select statements can be made to identify is a channel is being archived or not. An additional `isArchiving` columns could be used
to simply statue report clients.
