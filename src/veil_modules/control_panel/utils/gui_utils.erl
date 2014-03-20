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
-include("veil_modules/control_panel/common.hrl").
-include("logging.hrl").

-export([get_requested_hostname/0, get_requested_page/0, get_user_dn/0]).
-export([user_logged_in/0, storage_defined/0, dn_and_storage_defined/0, can_view_logs/0]).
-export([redirect_to_login/1, redirect_from_login/0]).
-export([apply_or_redirect/3, apply_or_redirect/4, top_menu/1, top_menu/2, logotype_footer/1, empty_page/0]).
-export([to_list/1, to_binary/1]).


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
    try
        Teams = user_logic:get_teams(wf:session(user_doc)),
        lists:foldl(
            fun(Team, Acc) ->
                Acc orelse (string:str(Team, "plggveilfs") > 0)
            end, false, Teams)
    catch _:_ ->
        false
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
            <<"Server encountered an unexpected error. Please contact the site administrator if the problem persists.">>)
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
%% Submenu body (list of nitrogen elements) will be concatenated below the main menu.
%% @end
-spec top_menu(ActiveTabID :: any(), SubMenuBody :: any()) -> list().
%% ====================================================================
top_menu(ActiveTabID, SubMenuBody) ->
    % Tab, that will be displayed optionally
    LogsPageCaptions = case can_view_logs() of
                           false ->
                               [];
                           true ->
                               [{logs_tab, #li{body = [
                                   #link{style = <<"padding: 18px;">>, url = <<"/logs">>, body = <<"Logs">>}
                               ]}}]
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
        ] ++ LogsPageCaptions,

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


%% to_list/1
%% ====================================================================
%% @doc Converts any term to list.
%% @end
-spec to_list(term()) -> list().
%% ====================================================================
to_list(Term) when is_list(Term) -> Term;
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





