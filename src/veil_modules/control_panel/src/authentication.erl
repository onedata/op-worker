-module(authentication).
-compile(export_all).

try_logging(Username, Password) ->
	{ok, Terms} = file:consult("src/veil_modules/control_panel/src/users.txt"),
	Tuple = lists:keyfind(Username, 1, Terms),
	case Tuple of 
		{Username, Password, Role} ->
			{ok, Role};
		_ ->
			{denied}
	end.