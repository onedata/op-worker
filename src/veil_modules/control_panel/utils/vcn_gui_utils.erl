%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains useful functions commonly used in
%% veil_cluster_node GUI modules.
%% @end
%% ===================================================================

-module(vcn_gui_utils).
-include("veil_modules/control_panel/common.hrl").
-include("logging.hrl").

% Functions connected with user's session
-export([get_user_dn/0, storage_defined/0, dn_and_storage_defined/0, can_view_logs/0, can_view_monitoring/0]).

% Saving and retrieving information that does not change during one session
-export([set_user_fullname/1, get_user_fullname/0, set_user_role/1, get_user_role/0, gen_logout_token/0, set_logout_token/1, get_logout_token/0]).

% Functions to check for user's session
-export([apply_or_redirect/3, apply_or_redirect/4, maybe_redirect/4]).

% Functions to generate page elements
-export([top_menu/1, top_menu/2, logotype_footer/1, empty_page/0]).


%% ====================================================================
%% API functions
%% ====================================================================

%% get_user_dn/0
%% ====================================================================
%% @doc Returns user's DN retrieved from his session state.
%% @end
-spec get_user_dn() -> string().
%% ====================================================================
get_user_dn() ->
    try
        {ok, UserDoc} = user_logic:get_user({login, gui_ctx:get_user_id()}),
        case user_logic:get_dn_list(UserDoc) of
            [] -> undefined;
            L when is_list(L) -> lists:nth(1, L);
            _ -> undefined
        end
    catch _:_ ->
        undefined
    end.


%% set_user_fullname/1
%% ====================================================================
%% @doc Sets user's full name in his session state.
%% @end
-spec set_user_fullname(Fullname :: string()) -> string().
%% ====================================================================
set_user_fullname(Fullname) ->
    wf:session(fullname, Fullname).


%% get_user_fullname/0
%% ====================================================================
%% @doc Returns user's full name retrieved from his session state.
%% @end
-spec get_user_fullname() -> string().
%% ====================================================================
get_user_fullname() ->
    wf:session(fullname).


%% set_user_role/1
%% ====================================================================
%% @doc Sets user's role in his session state.
%% @end
-spec set_user_role(Role :: atom()) -> atom().
%% ====================================================================
set_user_role(Role) ->
    wf:session(role, Role).


%% get_user_role/0
%% ====================================================================
%% @doc Returns user's role retrieved from his session state.
%% @end
-spec get_user_role() -> atom().
%% ====================================================================
get_user_role() ->
    wf:session(role).


%% gen_logout_token/0
%% ====================================================================
%% @doc Returns random character sequence that is used to verify user
%% logout.
%% @end
-spec gen_logout_token() -> binary().
%% ====================================================================
gen_logout_token() ->
    base64:encode(crypto:rand_bytes(20)).


%% get_logout_token/0
%% ====================================================================
%% @doc Returns user's logout token retrieved from his session state.
%% @end
-spec get_logout_token() -> binary().
%% ====================================================================
get_logout_token() ->
    wf:session(logout_token).


%% set_logout_token/1
%% ====================================================================
%% @doc Sets user's logout token in his session state.
%% @end
-spec set_logout_token(Token :: binary()) -> atom().
%% ====================================================================
set_logout_token(Token) ->
    wf:session(logout_token, Token).


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
    get_user_role() /= user.


%% can_view_monitoring/0
%% ====================================================================
%% @doc Determines if current user is allowed to view cluster monitoring.
%% @end
-spec can_view_monitoring() -> boolean().
%% ====================================================================
can_view_monitoring() ->
    get_user_role() /= user.


%% maybe_redirect/4
%% ====================================================================
%% @doc Decides if user can view the page, depending on arguments.
%% Returns false if no redirection is needed.
%% Otherwise, it issues a redirection and returns true.
%% Setting "SaveSourcePage" on true will allow a redirect back from login.
%% NOTE: Should be called from page:main().
%% @end
-spec maybe_redirect(NeedLogin :: boolean(), NeedDN :: boolean(), NeedStorage :: boolean(), SaveSourcePage :: boolean()) -> ok.
%% ====================================================================
maybe_redirect(NeedLogin, NeedDN, NeedStorage, SaveSourcePage) ->
    case NeedLogin and (not gui_ctx:user_logged_in()) of
        true ->
            gui_jq:redirect_to_login(),
            true;
        false ->
            case NeedDN and (vcn_gui_utils:get_user_dn() =:= undefined) of
                true ->
                    gui_jq:redirect(<<"/manage_account">>),
                    true;
                false ->
                    case NeedStorage and (not vcn_gui_utils:storage_defined()) of
                        true ->
                            gui_jq:redirect(<<"/manage_account">>),
                            true;
                        false ->
                            false
                    end
            end
    end.


%% apply_or_redirect/3
%% ====================================================================
%% @doc Checks if the client has right to do the operation (is logged in and possibly 
%% has a certificate DN defined). If so, it executes the code.
%% @end
-spec apply_or_redirect(Module :: atom, Fun :: atom, NeedDN :: boolean()) -> boolean().
%% ====================================================================
apply_or_redirect(Module, Fun, NeedDN) ->
    apply_or_redirect(Module, Fun, [], NeedDN).

%% apply_or_redirect/4
%% ====================================================================
%% @doc Checks if the client has right to do the operation (is logged in and possibly 
%% has a certificate DN defined). If so, it executes the code.
%% @end
-spec apply_or_redirect(Module :: atom, Fun :: atom, Args :: list(), NeedDN :: boolean()) -> boolean() | no_return.
%% ====================================================================
apply_or_redirect(Module, Fun, Args, NeedDN) ->
    try
        case gui_ctx:user_logged_in() of
            false ->
                gui_jq:redirect_to_login();
            true ->
                case NeedDN and (not dn_and_storage_defined()) of
                    true -> gui_jq:redirect(<<"/manage_account">>);
                    false -> erlang:apply(Module, Fun, Args)
                end
        end
    catch Type:Message ->
        ?error_stacktrace("Error in ~p - ~p:~p", [Module, Type, Message]),
        page_error:redirect_with_error(?error_internal_server_error),
        case gui_comet:is_comet_process() of
            true ->
                gui_comet:flush();
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
    PageCaptions =
        case can_view_monitoring() of
            false -> [];
            true -> [{monitoring_tab, #li{body = [
                #link{style = <<"padding: 18px;">>, url = <<"/monitoring">>, body = <<"Monitoring">>}
            ]}}]
        end ++
        case can_view_logs() of
            false -> [];
            true -> [
                {cluster_logs_tab, #li{body = [
                    #link{style = <<"padding: 18px;">>, url = <<"/cluster_logs">>, body = <<"Cluster logs">>}
                ]}},
                {client_logs_tab, #li{body = [
                    #link{style = <<"padding: 18px;">>, url = <<"/client_logs">>, body = <<"Client logs">>}
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
                url = <<"/manage_account">>, body = [gui_str:unicode_list_to_binary(get_user_fullname()), #span{class = <<"fui-user">>,
                    style = <<"margin-left: 10px;">>}]}}},
            %{contact_support_tab, #li { body=#link{ style="padding: 18px;", title="Contact & Support",
            %    url="/contact_support", body=#span{ class="fui-question" } } } },
            {about_tab, #li{body = #link{style = <<"padding: 18px;">>, title = <<"About">>,
                url = <<"/about">>, body = #span{class = <<"fui-info">>}}}},
            {logout_button, #li{
                body = #form{
                    id = <<"logout_form">>,
                    style = <<"margin: 0; padding: 18px;">>,
                    method = <<"POST">>,
                    action = <<"/logout">>,
                    body = [
                        #textbox{
                            style = <<"display: none">>,
                            name = ?logout_token,
                            value = vcn_gui_utils:get_logout_token()
                        },
                        #link{
                            style = <<"font-size: 24px;">>,
                            class = <<"glyph-link">>,
                            data_fields = [{<<"onclick">>, <<"document.getElementById('logout_form').submit(); return false;">>}],
                            title = <<"Log out">>,
                            body = #span{class = <<"fui-power">>}
                        }
                    ]
                }}
            }
%%                 #li{body = #link{style = <<"padding: 18px;">>, title = <<"Log out">>,
%%                 url = <<"/logout">>, body = #span{class = <<"fui-power">>}}}}
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

    [
        #panel{class = <<"navbar navbar-fixed-top">>, body = [
            #panel{class = <<"navbar-inner">>, style = <<"border-bottom: 2px solid gray;">>, body = [
                #panel{class = <<"container">>, body = [
                    #list{class = <<"nav pull-left">>, body = MenuCaptionsProcessed},
                    #list{class = <<"nav pull-right">>, body = MenuIconsProcessed}
                ]}
            ]}
        ] ++ SubMenuBody}
    ] ++ gui_utils:cookie_policy_popup_body(?privacy_policy_url).


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





