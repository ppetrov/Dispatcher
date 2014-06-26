using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;

namespace Dispatcher
{
	public abstract class Dispatcher<TItem, TResult> : IDisposable
	{
		private bool _disposed;

		private readonly IExceptLogger _logger;
		private readonly object _sync = new object();
		private readonly ManualResetEventSlim _stopEvent = new ManualResetEventSlim(false);
		private readonly ManualResetEventSlim _shutdownCompledEvent = new ManualResetEventSlim(false);

		public int Workers { get; set; }
		public TimeSpan QueryInterval { get; set; }

		protected abstract IEnumerable<TItem> GetItems();

		protected abstract TResult Send(TItem item);

		protected abstract void Save(TResult result);

		protected Dispatcher(IExceptLogger logger)
		{
			if (logger == null) throw new ArgumentNullException("logger");

			_logger = logger;
			this.Workers = 4;
			this.QueryInterval = TimeSpan.FromMinutes(1);
		}

		~Dispatcher()
		{
			this.Dispose(false);
		}

		public void Start()
		{
			ThreadPool.QueueUserWorkItem(_ =>
			{
				try
				{
					this.Dispatch();
				}
				catch (Exception ex)
				{
					_logger.Log(ex);
				}
			});
		}

		public void Stop()
		{
			_stopEvent.Set();
			_shutdownCompledEvent.Wait();
		}

		private void Dispatch()
		{
			var arguments = new object[2];

			while (!_stopEvent.IsSet)
			{
				var totalItems = 0;

				using (var completed = new ManualResetEventSlim(false))
				{
					using (var buffer = new BlockingCollection<TItem>(byte.MaxValue))
					{
						arguments[0] = buffer;
						arguments[1] = completed;

						ThreadPool.QueueUserWorkItem(this.DispatchBuffer, arguments);
						try
						{
							foreach (var item in this.GetItems())
							{
								buffer.Add(item);
								totalItems++;
							}
						}
						finally
						{
							buffer.CompleteAdding();
						}
						completed.Wait();
					}
				}
				if (totalItems == 0 && !_stopEvent.IsSet)
				{
					Thread.Sleep(this.QueryInterval);
				}
			}
			_shutdownCompledEvent.Set();
		}

		private void DispatchBuffer(object _)
		{
			var parameters = _ as object[];
			var buffer = parameters[0] as BlockingCollection<TItem>;
			var completed = parameters[1] as ManualResetEventSlim;
			var args = new object[this.Workers][];
			var events = new ManualResetEvent[this.Workers];

			try
			{
				for (var i = 0; i < events.Length; i++)
				{
					args[i] = new object[2];
					events[i] = new ManualResetEvent(true);
				}

				foreach (var item in buffer.GetConsumingEnumerable())
				{
					var index = WaitHandle.WaitAny(events);

					var e = events[index];
					e.Reset();

					var arg = args[index];
					arg[0] = item;
					arg[1] = e;

					ThreadPool.QueueUserWorkItem(this.ProcessItem, arg);
				}

				WaitHandle.WaitAll(events);
			}
			catch (Exception ex)
			{
				_logger.Log(ex);
			}
			finally
			{
				foreach (var mre in events)
				{
					if (mre != null)
					{
						mre.Dispose();
					}
				}
				completed.Set();
			}
		}

		private void ProcessItem(object arg)
		{
			var parameters = arg as object[];
			var item = (TItem)parameters[0];
			var mre = parameters[1] as ManualResetEvent;
			try
			{
				var result = this.Send(item);
				lock (_sync)
				{
					this.Save(result);
				}
			}
			catch (Exception ex)
			{
				_logger.Log(ex);
			}
			finally
			{
				mre.Set();
			}
		}

		protected virtual void Dispose(bool disposing)
		{
			if (_disposed)
				return;

			if (disposing)
			{
				_stopEvent.Dispose();
				_shutdownCompledEvent.Dispose();
			}

			_disposed = true;
		}

		public void Dispose()
		{
			this.Dispose(true);
			GC.SuppressFinalize(this);
		}
	}
}
