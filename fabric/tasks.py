from functools import wraps
from fabric import state
from fabric.network import interpret_host_string, disconnect_all
from fabric.utils import abort, indent
from fabric.job_queue import Job_Queue

def _get_runnable_method(task):
    """
    Determines if we are dealing with an old-style method or a new-style class and returns an appropiate callable
    """
    if _is_task(task):
        return task.run
    else:
        return task

def _is_task(task):
    """
    Is it a newstyle class ?
    """
    return hasattr(task, "run") and callable(task.run)

def execute(task, hosts, roles, *args, **kwargs):
    """
    Executes a command.
    If hosts and/or roles are specified, any decorated roles and/or hosts are discarded and the command is ran against the passed in hosts/roles
    If no hosts/roles are specified, the command is inspected for @hosts & @roles decorators and executed against these.
    """
    # Store previous state
    prev_attrs = [ "command", "all_hosts", "host_string", "host", "user", "port" ]
    prev_values = dict([(a, state.env[a]) for a in prev_attrs])

    # Set current command name, used for error messages
    if _is_task(task):
        state.env.command = task.name
    else:
        state.env.command = task.__name__

    # Get host list
    state.env.all_hosts = hosts = get_hosts(task, hosts, roles)
    try:
        for host in hosts:
            username, hostname, port = interpret_host_string(host)
            if state.output == "running":
                print("[%s] Executing task '%s'" % (host, state.env.command))
            if _is_task(task):
                task.run(*args, **kwargs)
            else:
                task(*args, **kwargs)
        if not hosts:
            # No hosts defined, so run command locally
            if _is_task(task):
                task.run(*args, **kwargs)
            else:
                task(*args, **kwargs)
    finally:
        disconnect_all()
    # Restore env
    state.env.update(prev_values)

def parallel_execute(task, hosts, roles, *args, **kwargs):
    """
    Executes a command in parallel on all defined hosts using the job_queue
    If hosts and/or roles are specified, any decorated roles and/or hosts are discarded and the command is ran against the passed in hosts/roles
    If no hosts/roles are specified, the command is inspected for @hosts & @roles decorators and executed against these.
    """
    # Store previous state
    import multiprocessing
    prev_attrs = [ "command", "all_hosts", "host_string", "host", "user", "port" ]
    prev_values = dict([(a, state.env[a]) for a in prev_attrs])

    # Set current command name, used for error messages
    if _is_task(task):
        state.env.command = task.name
    else:
        state.env.command = task.__name__

    # Get host list
    state.env.all_hosts = hosts = get_hosts(task, hosts, roles)
    # Determine poolsize
    if hasattr(task, "_pool_size") and task._pool_size:
        pool_size = task._pool_size
    elif not state.env.pool_size:
        pool_size = len(hosts)
    else:
        pool_size = state.env.pool_size
    # Resize pool_size to amount of hosts, to prevent overflow
    if pool_size > len(hosts):
        pool_size = len(hosts)
    # Create the jobqueue
    jobs = Job_Queue(max_running=pool_size)

    try:
        for host in hosts:
            username, hostname, port = interpret_host_string(host)
            if state.output == "running":
                print("[%s] Executing task '%s'" % (host, state.env.command))
            if _is_task(task):
                p = multiprocessing.Process(
                    target = task.run,
                    args = args,
                    kwargs = kwargs,
                    )
            else:
                p = multiprocessing.Process(
                    target=task,
                    args=args,
                    kwargs=kwargs
                )
            p.name = str(state.env.host_string)
            jobs.append(p)
        jobs.close()
        jobs.start()
        # Catch the local only case
        if not hosts:
            if _is_task(task):
                task.run(*args, **kwargs)
            else:
                task(*args, **kwargs)
    finally:
        disconnect_all()

    # Restore env
    state.env.update(prev_values)

def get_hosts(task, hosts, roles):
    """
    Return the host list the given command should be using.

    See :ref:`execution-model` for detailed documentation on how host lists are
    set.
    """
    # Command line per-command takes precedence over anything else.
    if hosts or roles:
        return _merge(hosts, roles)
    # Decorator-specific hosts/roles go next
    func_hosts = getattr(task, 'hosts', [])
    func_roles = getattr(task, 'roles', [])
    if func_hosts or func_roles:
        return _merge(func_hosts, func_roles)
    # Finally, the env is checked (which might contain globally set lists from
    # the CLI or from module-level code). This will be the empty list if these
    # have not been set -- which is fine, this method should return an empty
    # list if no hosts have been set anywhere.
    return _merge(state.env['hosts'], state.env['roles'])

def _merge(hosts, roles):
    """
    Merge given host and role lists into one list of deduped hosts.
    """
    # Abort if any roles don't exist
    bad_roles = [x for x in roles if x not in state.env.roledefs]
    if bad_roles:
        abort("The following specified roles do not exist:\n%s" % (
            indent(bad_roles)
        ))

    # Look up roles, turn into flat list of hosts
    role_hosts = []
    for role in roles:
        value = state.env.roledefs[role]
        # Handle "lazy" roles (callables)
        if callable(value):
            value = value()
        role_hosts += value
    # Return deduped combo of hosts and role_hosts
    return list(set(hosts + role_hosts))

class Task(object):
    """
    Abstract base class for objects wishing to be picked up as Fabric tasks.

    Instances of subclasses will be treated as valid tasks when present in
    fabfiles loaded by the :doc:`fab </usage/fab>` tool.

    For details on how to implement and use `~fabric.tasks.Task` subclasses,
    please see the usage documentation on :ref:`new-style tasks
    <new-style-tasks>`.

    .. versionadded:: 1.1
    """
    name = 'undefined'
    use_task_objects = True
    hosts = []
    roles = []
    force_sequential = False

    # TODO: make it so that this wraps other decorators as expected

    def run(self):
        raise NotImplementedError


class WrappedCallableTask(Task):
    """
    Wraps a given callable transparently, while marking it as a valid Task.

    Generally used via the `~fabric.decorators.task` decorator and not
    directly.

    .. versionadded:: 1.1
    """
    def __init__(self, callable):
        super(WrappedCallableTask, self).__init__()
        self.wrapped = callable
        self.__name__ = self.name = callable.__name__
        self.__doc__ = callable.__doc__

        # Wrap the @role/@hosts decorators as well ?
        if hasattr(callable, "roles"):
            self.roles = callable.roles
        if hasattr(callable, "hosts"):
            self.hosts = callable.hosts

    def __call__(self, *args, **kwargs):
        return self.run(*args, **kwargs)

    def run(self, *args, **kwargs):
        return self.wrapped(*args, **kwargs)

    def __getattr__(self, k):
        return getattr(self.wrapped, k)
