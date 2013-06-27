{-# LANGUAGE GeneralizedNewtypeDeriving #-}
module Pipes.Random
    ( randomSample
    , ReservoirT, runReservoirT
    , runReservoirP
    , execReservoirP
    ) where

--------------------------------------------------------------------------------
import Control.Applicative ((*>), (<$))
import Control.Monad ((>=>), when)
import Control.Monad.Morph (hoist)
import Control.Monad.Trans.Class (MonadTrans(..))
import Control.Monad.IO.Class (MonadIO(..))
import Data.Monoid (Dual(..))
import Data.Foldable (forM_)
import System.Random (RandomGen, randomR)


--------------------------------------------------------------------------------
import qualified Control.Monad.Trans.Writer.Strict as Writer
import qualified Data.IntMap as IntMap
import qualified Pipes as Pipes
import qualified Pipes.Lift as Pipes


--------------------------------------------------------------------------------
newtype ReservoirT a m r
    = ReservoirT { runReservoirT :: Writer.WriterT (Dual (IntMap.IntMap a)) m r }
  deriving (Monad, MonadTrans, MonadIO)


--------------------------------------------------------------------------------
randomSample
    :: (Monad m, RandomGen g)
    => Int -> g -> () -> Pipes.Consumer a (ReservoirT a m) g
randomSample n r () = establishReservoir *> thread (map go [n..]) r

  where

    establishReservoir =
        forM_ [1 .. n] $ \i -> Pipes.request () >>= (i .=)

    go t g =
        let (m, g') = randomR (1, t) g
        in g' <$ (Pipes.request () >>= when (m < n) . (m .=))

    i .= x = lift . ReservoirT . Writer.tell . Dual $ IntMap.singleton i x

    thread = foldr (>=>) return


--------------------------------------------------------------------------------
runReservoirP
    :: Monad m
    => Pipes.Proxy a' a b' b (ReservoirT a m) g
    -> Pipes.Proxy a' a b' b m (g, [a])
runReservoirP =
    fmap (fmap (IntMap.elems . getDual)) . Pipes.runWriterP .
        hoist runReservoirT


--------------------------------------------------------------------------------
execReservoirP
    :: Monad m
    => Pipes.Proxy a' a b' b (ReservoirT a m) g
    -> Pipes.Proxy a' a b' b m [a]
execReservoirP =
    fmap (IntMap.elems . getDual) . Pipes.execWriterP .
        hoist runReservoirT
