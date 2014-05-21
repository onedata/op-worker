%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains useful functions commonly used in control_panel modules.
%% @end
%% ===================================================================

-module(gui_utils).
-include_lib("ibrowse/include/ibrowse.hrl").
-include_lib("public_key/include/public_key.hrl").
-include("veil_modules/control_panel/common.hrl").
-include("logging.hrl").

% Functions connected with page / session context
-export([get_requested_hostname/0, get_requested_page/0, get_user_dn/0, get_request_params/0]).
% Functions connected with user's session
-export([user_logged_in/0, storage_defined/0, dn_and_storage_defined/0, can_view_logs/0, can_view_monitoring/0]).
% Functions used for redirecting to and from login
-export([redirect_to_login/1, redirect_from_login/0, maybe_redirect/4]).
% Functions to check for user's session and generate page elements
-export([apply_or_redirect/3, apply_or_redirect/4, top_menu/1, top_menu/2, logotype_footer/1, empty_page/0]).
% Comet API
-export([comet/1, init_comet/2, comet_supervisor/2, is_comet_process/0, flush/0]).
% Useful functions for binding custom events
-export([register_escape_event/1, script_for_enter_submission/2, script_to_bind_element_click/2]).
% jQuery wrappers for page updates
-export([update/2, replace/2, insert_top/2, insert_bottom/2, insert_before/2, insert_after/2, remove/1]).
% Conversion utils
-export([to_list/1, to_binary/1, join_to_binary/1]).
% Convinience function to set headers in cowboy response
-export([cowboy_ensure_header/3]).
% Functions used to perform secure server-server http requests
-export([https_get/2, https_post/3]).

%% Maximum redirects to follow when doing http request
-define(max_redirects, 5).
%% Maximum depth of CA cert analize
-define(ca_cert_max_depth, 11).


%% ====================================================================
%% API functions
%% ====================================================================

%% get_requested_hostname/0
%% ====================================================================
%% @doc Returns the hostname requested by the client.
%% @end
-spec get_requested_hostname() -> binary().
%% ====================================================================
get_requested_hostname() ->
    {Headers, _} = wf:headers(?REQ),
    proplists:get_value(<<"host">>, Headers, undefined).


%% get_requested_page/0
%% ====================================================================
%% @doc Returns the page requested by the client.
%% @end
-spec get_requested_page() -> binary().
%% ====================================================================
get_requested_page() ->
    Path = wf:path(?REQ),
    case Path of
        <<"/ws", Page/binary>> -> Page;
        <<Page/binary>> -> Page
    end.

%% get_user_dn/0
%% ====================================================================
%% @doc Returns user's DN retrieved from his session state.
%% @end
-spec get_user_dn() -> string().
%% ====================================================================
get_user_dn() ->
    try user_logic:get_dn_list(wf:session(user_doc)) of
        [] -> undefined;
        L when is_list(L) -> lists:nth(1, L);
        _ -> undefined
    catch _:_ ->
        undefined
    end.


%% get_request_params/0
%% ====================================================================
%% @doc Returns current http request params.
%% @end
-spec get_request_params() -> [tuple()].
%% ====================================================================
get_request_params() ->
    try
        ?CTX#context.params
    catch _:_ ->
        []
    end.

%% user_logged_in/0
%% ====================================================================
%% @doc Checks if the client has a valid login session.
%% @end
-spec user_logged_in() -> boolean().
%% ====================================================================
user_logged_in() ->
    (wf:user() /= undefined).


%% storage_defined/0
%% ====================================================================
%% @doc Checks if any storage is defined in the database.
%% @end
-spec storage_defined() -> boolean().
%% ====================================================================
storage_defined() ->
    case dao_lib:apply(dao_vfs, list_storage, [], 1) of
        {ok, []} -> false;
        {ok, L} when is_list(L) -> true;
        _ -> false
    end.


%% dn_and_storage_defined/0
%% ====================================================================
%% @doc Convienience function to check both conditions.
%% @end
-spec dn_and_storage_defined() -> boolean().
%% ====================================================================
dn_and_storage_defined() ->
    (get_user_dn() /= undefined) and storage_defined().


%% can_view_logs/0
%% ====================================================================
%% @doc Determines if current user is allowed to view cluster logs.
%% @end
-spec can_view_logs() -> boolean().
%% ====================================================================
can_view_logs() ->
    user_logic:get_role(wf:session(user_doc)) /= user.


%% can_view_monitoring/0
%% ====================================================================
%% @doc Determines if current user is allowed to view cluster monitoring.
%% @end
-spec can_view_monitoring() -> boolean().
%% ====================================================================
can_view_monitoring() ->
  user_logic:get_role(wf:session(user_doc)) /= user.


%% maybe_redirect/4
%% ====================================================================
%% @doc Decides if user can view the page, depending on arguments.
%% Returns false if no redirection is needed.
%% Otherwise, it issues a redirection and returns true.
%% Setting "SaveSourcePage" on true will allow a redirect back from login.
%% NOTE: Should be called from page:main().
%% @end
-spec maybe_redirect(boolean(), boolean(), boolean(), boolean()) -> ok.
%% ====================================================================
maybe_redirect(NeedLogin, NeedDN, NeedStorage, SaveSourcePage) ->
    case NeedLogin and (not user_logged_in()) of
        true ->
            gui_utils:redirect_to_login(SaveSourcePage),
            true;
        false ->
            case NeedDN and (get_user_dn() =:= undefined) of
                true ->
                    wf:redirect(<<"/manage_account">>),
                    true;
                false ->
                    case NeedStorage and (not storage_defined()) of
                        true ->
                            wf:redirect(<<"/manage_account">>),
                            true;
                        false ->
                            false
                    end
            end
    end.


%% redirect_to_login/1
%% ====================================================================
%% @doc Redirects to login page. Can remember the source page, so that
%% a user can be redirected back after logging in.
%% @end
-spec redirect_to_login(boolean()) -> ok.
%% ====================================================================
redirect_to_login(SaveSourcePage) ->
    PageName = get_requested_page(),
    case SaveSourcePage of
        false -> wf:redirect(<<"/login">>);
        true -> wf:redirect(<<"/login?x=", PageName/binary>>)
    end.


%% redirect_from_login/0
%% ====================================================================
%% @doc Redirects back from login page to the originally requested page.
%% If it hasn't been stored before, redirects to index page.
%% @end
-spec redirect_from_login() -> ok.
%% ====================================================================
redirect_from_login() ->
    case wf:q(<<"x">>) of
        undefined -> wf:redirect(<<"/">>);
        TargetPage -> wf:redirect(TargetPage)
    end.


%% apply_or_redirect/3
%% ====================================================================
%% @doc Checks if the client has right to do the operation (is logged in and possibly 
%% has a certificate DN defined). If so, it executes the code.
%% @end
-spec apply_or_redirect(Module :: atom, Fun :: atom, boolean()) -> boolean().
%% ====================================================================
apply_or_redirect(Module, Fun, NeedDN) ->
    apply_or_redirect(Module, Fun, [], NeedDN).

%% apply_or_redirect/4
%% ====================================================================
%% @doc Checks if the client has right to do the operation (is logged in and possibly 
%% has a certificate DN defined). If so, it executes the code.
%% @end
-spec apply_or_redirect(Module :: atom, Fun :: atom, Args :: list(), boolean()) -> boolean() | no_return.
%% ====================================================================
apply_or_redirect(Module, Fun, Args, NeedDN) ->
    try
        case user_logged_in() of
            false ->
                gui_utils:redirect_to_login(true);
            true ->
                case NeedDN and (not dn_and_storage_defined()) of
                    true -> wf:redirect(<<"/manage_account">>);
                    false -> erlang:apply(Module, Fun, Args)
                end
        end
    catch Type:Message ->
        ?error_stacktrace("Error in ~p - ~p:~p", [Module, Type, Message]),
        page_error:redirect_with_error(<<"Internal server error">>,
            <<"Server encountered an unexpected error. Please contact the site administrator if the problem persists.">>),
        case is_comet_process() of
            true ->
                flush();
            false ->
                skip
        end
    end.


%% top_menu/1
%% ====================================================================
%% @doc Convienience function to render top menu in GUI pages. 
%% Item with ActiveTabID will be highlighted as active.
%% @end
-spec top_menu(ActiveTabID :: any()) -> list().
%% ====================================================================
top_menu(ActiveTabID) ->
    top_menu(ActiveTabID, []).

%% top_menu/2
%% ====================================================================
%% @doc Convienience function to render top menu in GUI pages. 
%% Item with ActiveTabID will be highlighted as active. 
%% Submenu body (list of n2o elements) will be concatenated below the main menu.
%% @end
-spec top_menu(ActiveTabID :: any(), SubMenuBody :: any()) -> list().
%% ====================================================================
top_menu(ActiveTabID, SubMenuBody) ->
    % Tab, that will be displayed optionally
    LogsPageCaptions = case can_view_logs() of
                           false ->
                               [];
                           true ->
                               [
                                   {logs_tab, #li{body = [
                                        #link{style = <<"padding: 18px;">>, url = <<"/logs">>, body = <<"Logs">>}
                                   ]}},
                                   {monitoring_tab, #li{body = [
                                        #link{style = <<"padding: 18px;">>, url = <<"/monitoring">>, body = <<"Monitoring">>}
                                   ]}}
                               ]
                       end,
    % Define menu items with ids, so that proper tab can be made active via function parameter 
    % see old_menu_captions()
    MenuCaptions =
        [
            {file_manager_tab, #li{body = [
                #link{style = <<"padding: 18px;">>, url = <<"/file_manager">>, body = <<"File manager">>}
            ]}},
            {shared_files_tab, #li{body = [
                #link{style = <<"padding: 18px;">>, url = <<"/shared_files">>, body = <<"Shared files">>}
            ]}}
        ] ++ PageCaptions,

    MenuIcons =
        [
            {manage_account_tab, #li{body = #link{style = <<"padding: 18px;">>, title = <<"Manage account">>,
                url = <<"/manage_account">>, body = [user_logic:get_name(wf:session(user_doc)), #span{class = <<"fui-user">>,
                    style = <<"margin-left: 10px;">>}]}}},
            %{contact_support_tab, #li { body=#link{ style="padding: 18px;", title="Contact & Support",
            %    url="/contact_support", body=#span{ class="fui-question" } } } },
            {about_tab, #li{body = #link{style = <<"padding: 18px;">>, title = <<"About">>,
                url = <<"/about">>, body = #span{class = <<"fui-info">>}}}},
            {logout_button, #li{body = #link{style = <<"padding: 18px;">>, title = <<"Log out">>,
                url = <<"/logout">>, body = #span{class = <<"fui-power">>}}}}
        ],

    MenuCaptionsProcessed = lists:map(
        fun({TabID, ListItem}) ->
            case TabID of
                ActiveTabID -> ListItem#li{class = <<"active">>};
                _ -> ListItem
            end
        end, MenuCaptions),

    MenuIconsProcessed = lists:map(
        fun({TabID, ListItem}) ->
            case TabID of
                ActiveTabID -> ListItem#li{class = <<"active">>};
                _ -> ListItem
            end
        end, MenuIcons),

    #panel{class = <<"navbar navbar-fixed-top">>, body = [
        #panel{class = <<"navbar-inner">>, style = <<"border-bottom: 2px solid gray;">>, body = [
            #panel{class = <<"container">>, body = [
                #list{class = <<"nav pull-left">>, body = MenuCaptionsProcessed},
                #list{class = <<"nav pull-right">>, body = MenuIconsProcessed}
            ]}
        ]}
    ] ++ SubMenuBody}.


%% logotype_footer/1
%% ====================================================================
%% @doc Convienience function to render logotype footer, coming after page content.
%% @end
-spec logotype_footer(MarginTop :: integer()) -> list().
%% ====================================================================
logotype_footer(MarginTop) ->
    Height = integer_to_binary(MarginTop + 82),
    Margin = integer_to_binary(MarginTop),
    [
        #panel{style = <<"position: relative; height: ", Height/binary, "px;">>, body = [
            #panel{style = <<"text-align: center; z-index: -1; margin-top: ", Margin/binary, "px;">>, body = [
                #image{style = <<"margin: 10px 100px;">>, image = <<"/images/innow-gosp-logo.png">>},
                #image{style = <<"margin: 10px 100px;">>, image = <<"/images/plgrid-plus-logo.png">>},
                #image{style = <<"margin: 10px 100px;">>, image = <<"/images/unia-logo.png">>}
            ]}
        ]}
    ].


% Development functions
empty_page() ->
    [
        #h6{body = <<"Not yet implemented">>},
        #br{}, #br{}, #br{}, #br{}, #br{},
        #br{}, #br{}, #br{}, #br{}, #br{},
        #br{}, #br{}, #br{}, #br{}, #br{},
        #br{}, #br{}, #br{}, #br{}, #br{},
        #br{}, #br{}, #br{}, #br{}, #br{},
        #br{}, #br{}, #br{}, #br{}, #br{},
        #br{}, #br{}, #br{}, #br{}, #br{},
        #br{}, #br{}, #br{}, #br{}, #br{},
        #br{}, #br{}, #br{}, #br{}, #br{},
        #br{}, #br{}, #br{}, #br{}, #br{},
        #br{}, #br{}, #br{}, #br{}, #br{}
    ].


%% comet/1
%% ====================================================================
%% @doc Spawns an asynchronous process connected to the calling process.
%% IMPORTANT! The calling process must be the websocket process of n2o framework.
%% In other words, it should be called from event/1 function of page module.
%% Allows flushing actions to the main process (async updates).
%% Every instance of comet will get a supervisor to make sure it won't go rogue
%% after the calling process has finished.
%% @end
-spec comet(function()) -> {ok, pid()} | no_return().
%% ====================================================================
comet(CometFun) ->
    % Prevent comet and supervisor from killing the calling process on crash
    process_flag(trap_exit, true),
    % Spawn comet process, _link so it will die if the calling process craches
    CometPid = spawn_link(?MODULE, init_comet, [self(), CometFun]),
    % Spawn comet supervisor, _link so it will die if the calling process craches
    spawn_link(?MODULE, comet_supervisor, [self(), CometPid]),
    {ok, CometPid}.


%% init_comet/2
%% ====================================================================
%% @doc Internal function used to initialize an asynchronous "comet" process.
%% @end
-spec init_comet(pid(), fun()) -> no_return().
%% ====================================================================
init_comet(OwnerPid, Fun) ->
    timer:sleep(100), % defer the comet process so that n2o websocket process can initialize
    put(ws_process, OwnerPid),
    wf_context:init_context([]),
    Fun().


%% comet_supervisor/2
%% ====================================================================
%% @doc Internal function evaluated by comet supervisor. The supervisor will
%% kill the comet process whenever comet creator process finishes.
%% @end
-spec comet_supervisor(pid(), pid()) -> no_return().
%% ====================================================================
comet_supervisor(CallingPid, CometPid) ->
    MonitorRef = erlang:monitor(process, CallingPid),
    receive
        {'DOWN', MonitorRef, _, _, _} -> exit(CometPid, kill)
    end.


%% is_comet_process/0
%% ====================================================================
%% @doc Returns true if calling process is a comet process.
%% @end
-spec is_comet_process() -> boolean().
%% ====================================================================
is_comet_process() ->
    get(ws_process) /= undefined.


%% flush/0
%% ====================================================================
%% @doc Flushes accumulated events to websocket process, causing page update.
%% @end
-spec flush() -> ok.
%% ====================================================================
flush() ->
    Actions = wf_context:actions(),
    wf_context:clear_actions(),
    case Actions of
        [] ->
            skip;
        undefined ->
            skip;
        _ ->
            get(ws_process) ! {flush, Actions}
    end,
    ok.


%% register_escape_event/1
%% ====================================================================
%% @doc Binds escape button so that it generates an event every time it's pressed.
%% The event will call the function api_event(Tag, [], Context) on page module.
%% @end
-spec register_escape_event(string()) -> ok.
%% ====================================================================
register_escape_event(Tag) ->
    wf:wire(#api{name = "escape_pressed", tag = Tag}),
    wf:wire("$(document).bind('keydown', function (e){if (e.which == 27) escape_pressed();});").


%% script_for_enter_submission/2
%% ====================================================================
%% @doc Generates snippet of javascript, which can be directly used with wf:wire.
%% It intercepts enter keypresses on given input element and performs a click() on given submit button.
%% @end
-spec script_for_enter_submission(string(), string()) -> string().
%% ====================================================================
script_for_enter_submission(InputID, ButtonToClickID) ->
    wf:f("$('#~s').bind('keydown', function (e){ if (e.which == 13) { e.preventDefault(); $('#~s').click(); } });", [InputID, ButtonToClickID]).


%% script_to_bind_element_click/2
%% ====================================================================
%% @doc Generates snippet of javascript, which can be directly used with wf:wire.
%% It intercepts enter keypresses on given input element and performs a click() on given submit button.
%% @end
-spec script_to_bind_element_click(string(), string()) -> string().
%% ====================================================================
script_to_bind_element_click(ElementID, Javascript) ->
    wf:f("$('#~s').bind('click', function anonymous(event) { ~s });", [ElementID, Javascript]).


%% to_list/1
%% ====================================================================
%% @doc Converts any term to list.
%% @end
-spec to_list(term()) -> list().
%% ====================================================================
to_list(undefined) -> [];
to_list(Term) when is_list(Term) -> Term;
to_list(Term) when is_binary(Term) -> binary_to_list(Term);
to_list(Term) ->
    try
        wf:to_list(Term)
    catch _:_ ->
        lists:flatten(io_lib:format("~p", [Term]))
    end.


%% to_binary/1
%% ====================================================================
%% @doc Converts any term to binary.
%% @end
-spec to_binary(term()) -> binary().
%% ====================================================================
to_binary(Term) when is_binary(Term) -> Term;
to_binary(Term) -> list_to_binary(to_list(Term)).


%% join_to_binary/1
%% ====================================================================
%% @doc Joins any terms to a js-escaped binary.
%% @end
-spec join_to_binary([term()]) -> binary().
%% ====================================================================
join_to_binary(Terms) ->
    join_to_binary(Terms, <<"">>).

join_to_binary([], Acc) ->
    Acc;

join_to_binary([H | T], Acc) ->
    join_to_binary(T, <<Acc/binary, (to_binary(H))/binary>>).


% Set of update functions from n2o, slightly changed


%% update/2
%% ====================================================================
%% @doc Updates contents of a DOM element.
%% @end
-spec update(term(), term()) -> ok.
%% ====================================================================
update(Target, Elements) -> wf:wire(#jquery{target = Target, method = ["html"], args = [Elements], format = "'~s'"}).


%% replace/2
%% ====================================================================
%% @doc Replaces a DOM element with another.
%% @end
-spec replace(term(), term()) -> ok.
%% ====================================================================
replace(Target, Elements) ->
    wf:wire(#jquery{target = Target, method = ["replaceWith"], args = [Elements], format = "'~s'"}).


%% insert_top/2
%% ====================================================================
%% @doc Prepends an element to a DOM element.
%% @end
-spec insert_top(term(), term()) -> ok.
%% ====================================================================
insert_top(Target, Elements) ->
    wf:wire(#jquery{target = Target, method = ["prepend"], args = [Elements], format = "'~s'"}).


%% insert_bottom/2
%% ====================================================================
%% @doc Appends an element to a DOM element.
%% @end
-spec insert_bottom(term(), term()) -> ok.
%% ====================================================================
insert_bottom(Target, Elements) ->
    wf:wire(#jquery{target = Target, method = ["append"], args = [Elements], format = "'~s'"}).


%% insert_before/2
%% ====================================================================
%% @doc Inserts an element before a DOM element.
%% @end
-spec insert_before(term(), term()) -> ok.
%% ====================================================================
insert_before(Target, Elements) ->
    wf:wire(#jquery{target = Target, method = ["before"], args = [Elements], format = "'~s'"}).


%% insert_after/2
%% ====================================================================
%% @doc Inserts an element after a DOM element.
%% @end
-spec insert_after(term(), term()) -> ok.
%% ====================================================================
insert_after(Target, Elements) ->
    wf:wire(#jquery{target = Target, method = ["after"], args = [Elements], format = "'~s'"}).


%% remove/1
%% ====================================================================
%% @doc Updates an element from DOM.
%% @end
-spec remove(term()) -> ok.
%% ====================================================================
remove(Target) -> wf:wire(#jquery{target = Target, method = ["remove"]}).


%% cowboy_ensure_header/3
%% ====================================================================
%% @doc Sets a response header, but prevents duplicate entries. Header must
%% be normalized to lowercase (e. g. content-type and not Content-Type)
%% @end
-spec cowboy_ensure_header(binary(), binary(), req()) -> req().
%% ====================================================================
cowboy_ensure_header(Name, Value, Req) when is_binary(Name) and is_binary(Value) ->
    Req2 = cowboy_req:delete_resp_header(Name, Req),
    cowboy_req:set_resp_header(Name, Value, Req2).


%% https_get/2
%% ====================================================================
%% @doc Performs a HTTPS GET. Host is verified according to locally installed CA certs
%% (path is provided in environment variable). Only if connection is secure,
%% the request is performed.
%% @end
-spec https_get(binary() | string(), [{binary() | string(), binary() | string()}]) ->
    {ok, binary()} | {error, unknown_cert} | {error, term()}.
%% ====================================================================
https_get(URLBin, ReqHeadersBin) ->
    URL = gui_utils:to_list(URLBin),
    ReqHeaders = lists:map(
        fun({Key, Value}) ->
            {gui_utils:to_list(Key), gui_utils:to_list(Value)}
        end, ReqHeadersBin),
    perform_request(URL, ReqHeaders, get, "", ?max_redirects).


%% https_post/3
%% ====================================================================
%% @doc Performs a HTTPS POST. Host is verified according to locally installed CA certs
%% (path is provided in environment variable). Only if connection is secure,
%% the request is performed.
%% @end
-spec https_post(binary() | string(), [{binary() | string(), binary() | string()}], binary() | string()) ->
    {ok, binary()} | {error, unknown_cert} | {error, term()}.
%% ====================================================================
https_post(URLBin, ReqHeadersBin, Body) ->
    URL = gui_utils:to_list(URLBin),
    ReqHeaders = lists:map(
        fun({Key, Value}) ->
            {gui_utils:to_list(Key), gui_utils:to_list(Value)}
        end, ReqHeadersBin),
    %% 0 max redirects, according to RFC post requests should not be redirected
    perform_request(URL, ReqHeaders, post, gui_utils:to_list(Body), 0).

%% ====================================================================
%% Internal functions
%% ====================================================================

%% perform_request/4
%% ====================================================================
%% @doc Performs a HTTPS request with given args.
%% @end
-spec perform_request(string(), [{string(), string()}], atom(), binary(), integer()) ->
    {ok, binary()} | {error, unknown_cert} | {error, term()}.
%% ====================================================================
perform_request(URL, ReqHeaders, Method, Body, Redirects) ->
    try
        {ok, {_, _, Domain, _, _, _}} = http_uri:parse(URL),
        case ibrowse:send_req(URL, ReqHeaders, Method, Body, [{response_format, binary}, {ssl_options, ssl_opts(Domain)}]) of
            {ok, Rcode, RespHeaders, ResponseBody}
                when (Rcode =:= "301" orelse Rcode =:= "302" orelse Rcode =:= "303" orelse Rcode =:= "307") andalso Redirects > 0 ->
                % Code in {301, 302, 303, 307} - we are being redirected
                case get_redirect_url(URL, RespHeaders) of
                    undefined -> ResponseBody;
                    URL -> ResponseBody;
                    NewURL -> perform_request(NewURL, ReqHeaders, Method, Body, Redirects - 1)
                end;

            {ok, "200", _, ResponseBody} ->
                % Answer ok
                {ok, ResponseBody};

            {error, {conn_failed, {error, {tls_alert, "certificate unknown"}}}} ->
                % Host authenticity cannot be confirmed
                {error, unknown_cert};

            {error, Other} ->
                {error, Other};

            Other ->
                {error, Other}
        end
    catch
        _:M ->
            {error, M}
    end.


%% get_redirect_url/1
%% ====================================================================
%% @doc
%% Retrieves redirect URL from a HTTP response.
%% @end
-spec get_redirect_url(string(), list()) -> string().
%% ====================================================================
get_redirect_url(OldURL, Headers) ->
    Location = proplists:get_value("location", Headers, proplists:get_value("Location", Headers)),
    case Location of
        "http://" ++ _ -> Location;
        "https://" ++ _ -> Location;
        [$/ | _] = Location ->
            #url{protocol = Protocol, host = Host, port = Port} = ibrowse_lib:parse_url(OldURL),
            PortFrag = case {Protocol, Port} of
                           {http, 80} -> "";
                           {https, 443} -> "";
                           _ -> ":" ++ integer_to_list(Port)
                       end,
            atom_to_list(Protocol) ++ "://" ++ Host ++ PortFrag ++ Location;
        _ -> undefined
    end.


%% ssl_opts/1
%% ====================================================================
%% @doc Returns list of ssl opts for secure connection.
%% @end
-spec ssl_opts(string()) -> [tuple()].
%% ====================================================================
ssl_opts(ReqHostname) ->
    VerifyFun =
        fun(_, {bad_cert, _}, RequestedHostname) ->
            {unknown, RequestedHostname};

            (_, {extension, _}, RequestedHostname) ->
                {unknown, RequestedHostname};

            (_, valid, RequestedHostname) ->
                {valid, RequestedHostname};

            (Cert, valid_peer, RequestedHostname) ->
                % If peer is valid, make sure one of domain names contained in cert matches our requested adress
                #'OTPCertificate'{tbsCertificate = #'OTPTBSCertificate'{extensions = Extensions}} = Cert,
                AllowedHostnames = lists:foldl(
                    fun(#'Extension'{extnID = ExtID, extnValue = ExtVal}, Acc) ->
                        case ExtID of
                            ?'id-ce-subjectAltName' ->
                                Acc ++ lists:map(
                                    fun({dNSName, DNSName}) ->
                                        % Create regexps from allowed domain names, to later match them against requested address
                                        ReplacedDots = re:replace(DNSName, "\\.", "\\\\.", [global, {return, list}]),
                                        _ReplacedWildcards = re:replace(ReplacedDots, "\\*", ".*", [global, {return, list}])
                                    end, ExtVal);
                            _ ->
                                Acc
                        end
                    end, [], Extensions),

                Valid = lists:foldl(
                    fun(RegExp, Acc) ->
                        case re:run(RequestedHostname, RegExp) of
                        % At least one domain name matched, the peer is indeed valid
                            {match, _} -> valid;
                            _ -> Acc
                        end
                    end, unknown, AllowedHostnames),
                {Valid, RequestedHostname}
        end,

    CaCertFileAtom = case application:get_env(veil_cluster_node, root_cacert_file) of
                         {ok, Val} -> Val;
                         _ -> throw("root_cacert_file env missing")
                     end,
    % Return ssl opts for a secure connection
    [
        {verify, verify_peer},
        {cacertfile, atom_to_list(CaCertFileAtom)},
        {verify_fun, {VerifyFun, ReqHostname}},
        {depth, ?ca_cert_max_depth}
    ].


% old_menu_captions() ->
% _MenuCaptions =
%     [
%         {data_tab, #li { body=[
%             #link{ style="padding: 18px;", url="/file_manager", body="Data" },
%             #list { style="top: 37px;", body=[
%                 #li { body=#link{ url="/file_manager", body="File manager" } },
%                 #li { body=#link{ url="/shared_files", body="Shared files" } }
%             ]}
%         ]}},
%         {rules_tab, #li { body=[
%             #link{ style="padding: 18px;", url="/rules_composer", body="Rules" },
%             #list {  style="top: 37px;", body=[
%                 #li { body=#link{ url="/rules_composer", body="Rules composer" } },
%                 #li { body=#link{ url="/rules_viewer", body="Rules viewer" } },
%                 #li { body=#link{ url="/rules_simulator", body="Rules simulator" } }
%             ]}
%         ]}},
%         {administration_tab, #li { body=[
%             #link{ style="padding: 18px;", url="/system_state", body="Administration" },
%             #list {  style="top: 37px;", body=[
%                 #li { body=#link{ url="/system_state", body="System state" } },
%                 #li { body=#link{ url="/events", body="Events" } }
%             ]}
%         ]}}
%     ].





