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

-module(vcn_gui_utils).
-include("veil_modules/control_panel/common.hrl").
-include("logging.hrl").

% Functions connected with user's session
-export([get_user_dn/0, storage_defined/0, dn_and_storage_defined/0, can_view_logs/0, can_view_monitoring/0]).

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
    try user_logic:get_dn_list(gui_ctx:get_user_record()) of
        [] -> undefined;
        L when is_list(L) -> lists:nth(1, L);
        _ -> undefined
    catch _:_ ->
        undefined
    end.


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
    user_logic:get_role(gui_ctx:get_user_record()) /= user.


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
-spec maybe_redirect(NeedLogin :: boolean(), NeedDN :: boolean(), NeedStorage :: boolean(), SaveSourcePage :: boolean()) -> ok.
%% ====================================================================
maybe_redirect(NeedLogin, NeedDN, NeedStorage, SaveSourcePage) ->
    case NeedLogin and (not gui_ctx:user_logged_in()) of
        true ->
            gui_jq:redirect_to_login(SaveSourcePage),
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
                gui_jq:redirect_to_login(true);
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
        case can_view_logs() of
            false -> [];
            true -> [{logs_tab, #li{body = [
                #link{style = <<"padding: 18px;">>, url = <<"/logs">>, body = <<"Logs">>}
            ]}}]
        end ++
        case can_view_monitoring() of
            false -> [];
            true -> [{monitoring_tab, #li{body = [
                #link{style = <<"padding: 18px;">>, url = <<"/monitoring">>, body = <<"Monitoring">>}
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
        ] ++ PageCaptions,

    MenuIcons =
        [
            {manage_account_tab, #li{body = #link{style = <<"padding: 18px;">>, title = <<"Manage account">>,
                url = <<"/manage_account">>, body = [user_logic:get_name(gui_ctx:get_user_record()), #span{class = <<"fui-user">>,
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





