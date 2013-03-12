%% -*- mode: nitrogen -*-
-module (index).
-compile(export_all).
-include_lib("nitrogen_core/include/wf.hrl").
%-include("records.hrl").

main() -> 
    case wf:role("user") of
        true ->
            #template { file="src/veil_modules/control_panel/static/templates/user.html" };
        false ->
            case wf:role("admin") of
                true ->
                    #template { file="src/veil_modules/control_panel/static/templates/admin.html" };
                false ->
                    case wf:role("co") of
                        true ->
                            #template { file="src/veil_modules/control_panel/static/templates/co.html" };
                        false -> wf:redirect_to_login("/login")
                    end
            end
    end.

title() -> "Strona glowna".

header() ->
    #panel { style="margin: 10px; background-color: #8822ff;", body=[
        #button { text="Wyloguj", id=logout, postback=logout }
    ]}.

body(user) ->
    wf:comet_global(fun() -> registerUser() end, users),
    [
        #panel { style="margin: 20px;", id=mainpanel, body=
        [
            #h1 { text="PANEL UZYTKOWNIKA" }
        ]},
        #panel { style="margin: 20px; float: right; text-align: left; width: 600px;", id=sidepanel }
    ];

body(admin) ->
    wf:comet_global(fun() -> registerAdmin() end, users),
    [
        #panel { style="margin: 20px;", id=mainpanel, body=
        [
            #h1 { text="PANEL ADMINA" },
            #br { },
            #hr { },
            #label { text="Zalogowani uzytkownicy:" },
            #list { id=userlist, numbered=false, body=[] }
        ]},
        #panel { style="margin: 20px; float: right; text-align: left; width: 600px;", id=sidepanel }
    ];

body(co) ->
    wf:comet(fun() -> clock(0) end),
    #panel { style="margin: 20px;", body=[
        #h1 { text="PANEL CO" },
        #panel { body=
        [
            #label { text="Przebywasz na stronie:" }
        ] },
        #panel { id=clocklabel, body=
        [
            "0 sec." 
        ] }
    ]}.

event(logout) ->
    User = wf:user(),
    wf:logout(),
    wf:send_global(users, {logout, User}),
    wf:redirect_to_login("/login");

event({zagaj, Who, Message}) ->
    wf:send_global(users, {message, {Who, wf:user(), Message}}).

clock(N) ->
    timer:sleep(1000),
    Wynik1 = wf:update(clocklabel, [integer_to_list(N), " sec."]),
    Wynik2 = wf:flush(),
    io:format("~w: Flushed... ~w, ~w~n", [N, Wynik1, Wynik2]),
    clock(N + 1).

registerUser() ->    
    wf:send_global(users, {new, wf:user()}),
    process_flag(trap_exit, true),
    io:format("~p", [self()]),
    register(majonez, self()),
    userController(wf:user()).

userController(User) ->
    receive
        {'EXIT', _, _} ->
            wf:send_global(users, {left, wf:user()});

        {ping} ->
            wf:send_global(users, {new, wf:user()}),
            io:format("~s: Zostalem spingowany.~n", [wf:user()]),
            userController(User);

        {message, {User, Sender, Message}} ->
            wf:insert_top(sidepanel, ["<b>", Sender, "</b> says:<br />", Message, "<hr />"]),
            wf:flush(),
            userController(User);

        Cokolwiek ->
            io:format("~s: Odebralem: ~p.~n", [wf:user(), Cokolwiek]),
            userController(User)
    end.


registerAdmin() ->    
    timer:sleep(1000),
    wf:send_global(users, {ping}),
    userMonitor([], []).

userMonitor(UserList, OldList) ->

    case UserList =:= OldList of

        false ->
            Body = lists:map(fun({X}) -> #listitem { body= 
                [
                    X,
                    " ",
                    #button { text="Zagaj", postback={zagaj, X, "Jak leci?"} }                
                ] } end, lists:sort(UserList)),
            wf:update(userlist, [Body]),
            wf:flush(),
            userMonitor(UserList, UserList);

        true ->
            User = wf:user(),
            receive
                {ping} ->
                    wf:send_global(users, {new, wf:user()}),
                    io:format("~p: Spingowalem siebie.~n", [self()]),
                    userMonitor(UserList, OldList);

                {new, UserName} ->
                    io:format("~p: Nowy uzytkownik: ~s~n", [self(), UserName]),
                    case lists:member({UserName}, UserList) of
                        true -> 
                            io:format("~p: Aktualna lista: ~p~n", [self(), UserList]),
                            userMonitor(UserList, OldList);
                        false ->
                            io:format("~p: Aktualna lista: ~p~n", [self(), [{UserName}|UserList]]),
                            userMonitor([{UserName}|UserList], UserList)
                    end;

                {left, UserName} ->
                    io:format("~p: Sygnal o opuszczeniu: ~s~n", [self(), UserName]),
                    wf:send_global(users, {ping}),
                    userMonitor(lists:delete({UserName}, UserList), UserList);

                {logout, UserName} ->
                    io:format("~p: Sygnal o wylogowaniu: ~s~n", [self(), UserName]),
                    userMonitor(lists:delete({UserName}, UserList), UserList);

                {message, {User, Sender, Message}} ->
                    wf:insert_top(sidepanel, ["<b>", Sender, "</b> says:<br />", Message, "<hr />"]),
                    wf:flush(),
                    userMonitor(UserList, OldList);

                X ->
                    io:format("~s - odebralem: ~w~n", [wf:user(), X]),           
                    wf:flush(),
                    userMonitor(UserList, OldList)
            end
    end.