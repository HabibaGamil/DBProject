# Database-Engine
This project was part of our universities database course. We studied the internals of database engines and how data is handeled in terms of data structures ans supporting indices.
This project is a mini database engine, with basic functionalities that include creating tables, inserting, deleting, updating and selecting. This project supports Grid indices (with custom dimensions) which reduce search space for common select queries.
They could also be used to insert, update, delete more efficienty

 ## Guide
 
 
### Table Class
Class which keeps track of information related to each table. This includes pages belonging to this table as well as any indices created by the user.

 ### Page Class
Page is the object through which page files on the disk are serialized/deserialized, It also contains helper methods which search, insert, remove from page.

### Grid Class
Root class which contains info about the grid, this includes the columns on which the grid is made as well as reference to its cells.

### Cell Class
Each cell object lies in a specific dimension, it either contains a reference to the next dimension through a cell array or would contain a reference to the bucket which contains pointers to tuples in that cell

### Bucket Class
Bucket is the object through which bucket files on the disk are serialized/deserialized, It also contains helper methods which search, insert, remove from buckets.

### Hashtuples Class
A modified version of Hashtable class which was modified to implement iterator. This allows the applications to run select queries efficiently as looping on results are avoided by insuring hashtable doesn't contain dublicates.

### Bonuses
-This project executes select queries with precedence as (AND,OR,XOR) -This engine supports partial select queries along with full select queries -This engine makes use of grid index (if present) to reduce search space in insertion, deletion and update.
