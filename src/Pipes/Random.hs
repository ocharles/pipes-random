module Pipes.Random (randomSample) where

import Control.Applicative ((*>), (<$))
import Control.Monad ((>=>), when)
import Control.Monad.Trans.Class (lift)
import Data.Monoid (Dual(..))
import Data.Foldable (forM_)
import System.Random (RandomGen, randomR)

import qualified Control.Monad.Trans.Writer.Strict as Writer
import qualified Data.IntMap as IntMap
import qualified Pipes as Pipes

randomSample
    :: (Monad m, RandomGen g)
    => Int -> g
    -> () -> Pipes.Consumer a (Writer.WriterT (Dual (IntMap.IntMap a)) m) g
randomSample n r () = establishReservoir *> thread (map go [n..]) r

  where

    establishReservoir =
        forM_ [1 .. n] $ \i -> Pipes.request () >>= (i .=)

    go t g =
        let (m, g') = randomR (1, t) g
        in g' <$ (Pipes.request () >>= when (m < n) . (m .=))

    i .= x = lift $ Writer.tell (Dual $ IntMap.singleton i x)

    thread = foldr (>=>) return
