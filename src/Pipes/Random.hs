{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-| @pipes-random@ provides 'Pipes.Producer's for generating infinite streams of
    'Random' data, and also an implementation of reservoir sampling to take @n@
    samples from any upstream producer. The implementation for reservoir sampling
    is the first algorithm in Jeffrey Vitter's paper \"Random Sampling with a
    Reservoir\" - <http://www.cs.umd.edu/~samir/498/vitter.pdf>.
-}
module Pipes.Random
    ( -- * Random 'Pipes.Producer's
      random
    , randomR

      -- * Random sampling
    , randomSample
    , runReservoirP
    , execReservoirP

      -- * Internals
    , ReservoirT, runReservoirT
    ) where

--------------------------------------------------------------------------------
import Control.Applicative ((*>), (<$))
import Control.Monad ((>=>), when)
import Control.Monad.Morph (hoist)
import Control.Monad.Trans.Class (MonadTrans(..))
import Control.Monad.IO.Class (MonadIO(..))
import Data.Monoid (Dual(..))
import Data.Foldable (forM_)
import System.Random (Random, RandomGen)


--------------------------------------------------------------------------------
import qualified Control.Monad.Trans.Writer.Strict as Writer
import qualified Data.IntMap as IntMap
import qualified Pipes as Pipes
import qualified Pipes.Lift as Pipes
import qualified System.Random as Random


--------------------------------------------------------------------------------
newtype ReservoirT a m r
    = ReservoirT { runReservoirT :: Writer.WriterT (Dual (IntMap.IntMap a)) m r }
  deriving (Monad, MonadTrans, MonadIO)


--------------------------------------------------------------------------------
-- | Produce an infinite stream of random values.
random :: (Monad m, Random a, RandomGen g) => g -> Pipes.Producer a m ()
random r = Pipes.each (Random.randoms r)


--------------------------------------------------------------------------------
-- | Produce an infinite stream of random values within a range.
randomR
    :: (Monad m, Random a, RandomGen g)
    => (a, a) -> g -> Pipes.Producer a m ()
randomR range r = Pipes.each (Random.randomRs range r)


--------------------------------------------------------------------------------
-- | @randomSample n@ takes n random samples from upstream, aggregating them in
-- 'ReservoirT'. Run the resulting proxy with 'runReservoirP' or
-- 'execReservoirP' to access the final @n@ random samples.
randomSample
    :: (Monad m, RandomGen g)
    => Int -> g -> Pipes.Consumer a (ReservoirT a m) g
randomSample n r = establishReservoir *> thread (map go [n..]) r

  where

    establishReservoir =
        forM_ [1 .. n] $ \i -> Pipes.await >>= (i .=)

    go t g =
        let (m, g') = Random.randomR (1, t) g
        in g' <$ (Pipes.await >>= when (m < n) . (m .=))

    i .= x = lift . ReservoirT . Writer.tell . Dual $ IntMap.singleton i x


--------------------------------------------------------------------------------
-- | Run a 'ReservoirT' 'Pipes.Proxy' to gain access to the final 'RandomGen'
-- and a list of random values.
runReservoirP
    :: Monad m
    => Pipes.Proxy a' a b' b (ReservoirT v m) g
    -> Pipes.Proxy a' a b' b m (g, [v])
runReservoirP =
    fmap (fmap (IntMap.elems . getDual)) . Pipes.runWriterP .
        hoist runReservoirT


--------------------------------------------------------------------------------
-- | Run a 'ReservoirT' 'Pipes.Proxy' discarding the final 'RandomGen' and
-- returning the list of random values.
execReservoirP
    :: Monad m
    => Pipes.Proxy a' a b' b (ReservoirT v m) g
    -> Pipes.Proxy a' a b' b m [v]
execReservoirP =
    fmap (IntMap.elems . getDual) . Pipes.execWriterP .
        hoist runReservoirT


--------------------------------------------------------------------------------
thread :: Monad m => [a -> m a] -> a -> m a
thread = foldr (>=>) return
