# POC_BroadRidge

The given task is very generic and am asked to join two datasets from the given github link

I could not find related datasets so I split the crm_data which has mbr attributes as well as risk and sentiment fields

I created two hive table one for mbr and the other for mbr risk partitioned on id_firstchar field

I read the file into spark data frame, did some processing to change the data types of date fiels and add id_firstchar field

I used the derived field id_first char  as it has good distribution of data and no significant skewness of data

I read the data by joiniing these two table and did some minor analysis.



sample data from analysis:

+-----+------+--------+-------+--------+
|state|gender|NEGATIVE|NEUTRAL|POSITIVE|
+-----+------+--------+-------+--------+
|   ak|FEMALE|      25|     55|      67|
|   ak|  MALE|      22|     53|      50|
|   ak| OTHER|       1|      2|       1|
|   al| OTHER|       4|     10|      10|
|   al|FEMALE|     101|    347|     234|
|   al|  MALE|     100|    405|     244|
|   ar| OTHER|       2|      4|       4|
|   ar|  MALE|      81|    245|     150|
|   ar|FEMALE|      79|    247|     160|
|   az|FEMALE|     154|    490|     369|
|   az| OTHER|       8|     20|      15|
|   az|  MALE|     164|    496|     363|
|   ca|FEMALE|     918|   2864|    2049|
|   ca| OTHER|      30|    110|      86|
|   ca|  MALE|     947|   2924|    2123|
|   co|FEMALE|     106|    471|     294|
|   co|  MALE|     114|    362|     272|
|   co| OTHER|       4|     27|      13|
|   ct|FEMALE|      78|    284|     233|
|   ct|  MALE|      74|    304|     179|
|   ct| OTHER|       2|      7|       3|
|   dc| OTHER|    null|      7|       2|
|   dc|FEMALE|      20|     37|      32|
|   dc|  MALE|      16|     57|      29|
|   de|  MALE|      24|     66|      58|
|   de|FEMALE|      31|     59|      56|
|   de| OTHER|    null|      3|       2|
|   fl| OTHER|      19|     64|      44|
|   fl|  MALE|     480|   1523|     993|
|   fl|FEMALE|     443|   1542|    1056|
|   ga|FEMALE|     226|    771|     542|
|   ga| OTHER|       5|     42|      24|
|   ga|  MALE|     237|    822|     528|
|   hi| OTHER|    null|      5|       4|
|   hi|  MALE|      33|    115|      78|
|   hi|FEMALE|      32|    102|      77|
|   ia| OTHER|    null|     10|       7|
|   ia|  MALE|      65|    223|     130|
|   ia|FEMALE|      58|    248|     173|
|   id|FEMALE|      35|    133|      93|
|   id| OTHER|       2|      4|       1|
|   id|  MALE|      35|     99|      75|
|   il| OTHER|      13|     47|      25|
|   il|  MALE|     342|   1040|     729|
|   il|FEMALE|     277|    999|     647|
|   in|  MALE|     171|    480|     361|
|   in| OTHER|       7|     26|      19|
|   in|FEMALE|     153|    547|     380|
|   ks| OTHER|       1|      6|       3|
|   ks|  MALE|      59|    220|     145|
|   ks|FEMALE|      74|    236|     158|
