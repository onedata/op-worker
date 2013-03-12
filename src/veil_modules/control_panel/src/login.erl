%% -*- mode: nitrogen -*-
-module (login).
-compile(export_all).
-include_lib("nitrogen_core/include/wf.hrl").
%-include("records.hrl").

main() -> 
    case wf:role("user") or wf:role("admin") or wf:role("co") of
        true -> 
            wf:redirect_from_login("/");
        false ->
            #template { file="src/veil_modules/control_panel/static/templates/bare.html" }
    end.

title() -> "Strona logowania".

body() -> 
    #panel { style="margin: 50px;", body=[
        #flash {},
        #label { text="Username" },
        #textbox { id=username, next=password },
        #br {},
        #label { text="Password" },
        #password { id=password, next=submit },
        #br {},
        #button { text="Login", id=submit, postback=login }
    ]}.

event(login) ->
    Username = wf:q(username),
    Password = wf:q(password),    
    case authentication:try_logging(Username, Password) of
        {ok, Role} ->
            wf:role(Role, true),
            wf:user(Username ++ " (" ++ Role ++ ")"),
            wf:redirect_from_login("/");
        _ ->
            wf:flash("Invalid user or password.")
    end.