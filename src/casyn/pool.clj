(ns casyn.pool
  "Protocols defining what is expected from different pool/client
   implementations")

(defprotocol PPool
  (borrow [pool node-host] "Get a valid/idle client from the pool, return a result-channel")
  (return [pool node-host client] "Get a healthy client to the pool, return a result-channel")
  (invalidate [pool node-host client] "flag a client as unsafe")
  (return-or-invalidate [pool node-host client] "Get a healthy client to the pool, return a result-channel")
  (add [pool node-host] "Add idle/connected client to the pool")
  (drain [pool] [pool node-host] "")
  (close [pool] "Clear and close the pool")
  (active-clients [pool] [pool node-host] "")
  (idle-clients [pool] [pool node-host] ""))

(defprotocol PPoolableClient
  (borrowable? [client] "Health check client before borrow")
  (returnable? [client] "Health check client before it is returned"))
