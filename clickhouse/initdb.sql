CREATE TABLE fromjson (
   datedate Date,
   ts Int64,
   userId Int64,
   sessionId Int64,
   page String,
   auth String,
   method String,
   status UInt16,
   level String,
   itemInSession UInt16,
   location String,
   userAgent String,
   lastName String,
   firstName String,
   registration UInt64,
   gender String,
   artist String,
   song String,
   length Float64
) ENGINE = MergeTree(datedate, userId, 8192);


CREATE TABLE aggjson (
   datedate Date,
   ts Int64,
   userId Int64,
   sessionId Int64,
   page String,
   auth String,
   method String,
   status UInt16,
   level String,
   itemInSession UInt16,
   location String,
   userAgent String,
   lastName String,
   firstName String,
   registration UInt64,
   gender String,
   artist String,
   song String,
   length Float64
) ENGINE = MergeTree(datedate, userId, 8192);
