module Pipes.Random
    ( randomSample
    , runRandomP
    , execRandomP
    ) where

--------------------------------------------------------------------------------
import Control.Applicative ((*>), (<$))
import Control.Monad ((>=>), when)
import Control.Monad.Trans.Class (lift)
import Data.Monoid (Dual(..))
import Data.Foldable (forM_)
import System.Random (RandomGen, randomR)


--------------------------------------------------------------------------------
import qualified Control.Monad.Trans.Writer.Strict as Writer
import qualified Data.IntMap as IntMap
import qualified Pipes as Pipes
import qualified Pipes.Lift as Pipes


--------------------------------------------------------------------------------
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


--------------------------------------------------------------------------------
runRandomP
    :: Monad m
    => Pipes.Proxy a' a b' b (Writer.WriterT (Dual (IntMap.IntMap a)) m) g
    -> Pipes.Proxy a' a b' b m (g, [a])
runRandomP = fmap (fmap (IntMap.elems . getDual)) . Pipes.runWriterP


--------------------------------------------------------------------------------
execRandomP
    :: Monad m
    => Pipes.Proxy a' a b' b (Writer.WriterT (Dual (IntMap.IntMap a)) m) g
    -> Pipes.Proxy a' a b' b m [a]
execRandomP = fmap (IntMap.elems . getDual) . Pipes.execWriterP
