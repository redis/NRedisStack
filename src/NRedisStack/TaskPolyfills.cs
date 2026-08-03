// ReSharper disable once CheckNamespace
namespace System.Threading.Tasks;

#if !NET // modern .NET has this on Task itself; NS/NETFX do not
internal static class TaskPolyfills
{
    extension(Task task)
    {
        /// <summary>
        /// Gets whether the task completed successfully, as opposed to faulting, being cancelled, or still
        /// being pending.
        /// </summary>
        public bool IsCompletedSuccessfully => task.Status == TaskStatus.RanToCompletion;
    }
}
#endif
