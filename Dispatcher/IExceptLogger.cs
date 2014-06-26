using System;

namespace Dispatcher
{
	public interface IExceptLogger
	{
		void Log(Exception exception);
	}
}