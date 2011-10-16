import Database.Altis.ServerProtocol ( runAltisServer )
import Control.Monad (forever)
import Control.Concurrent (threadDelay)
import Control.Concurrent.STM (newTVarIO)

import qualified Data.Map as M


main = do
    db <- newTVarIO M.empty
    runAltisServer db
    forever $ threadDelay 1000000000
