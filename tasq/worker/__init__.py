from .jobqueue import JobQueue
from .actors import WorkerActor
from .executor import ProcessQueueExecutor
from ..actors.actorsystem import get_actorsystem


def build_worker_actor_router(
    router_class, num_workers, ctx=get_actorsystem("")
):
    return ctx.router_of(
        num_workers=num_workers,
        actor_class=WorkerActor,
        router_class=router_class,
    )


def build_jobqueue(num_workers):
    return JobQueue(num_workers=num_workers, worker_class=ProcessQueueExecutor)
