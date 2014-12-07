%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains n2o website code.
%% The page allows user to manage his Spaces.
%% @end
%% ===================================================================

-module(page_spaces).
-include("oneprovider_modules/control_panel/common.hrl").
-include_lib("ctool/include/global_registry/gr_spaces.hrl").
-include_lib("ctool/include/logging.hrl").

% n2o API and comet
-export([main/0, event/1, api_event/3, comet_loop/1]).

%% Comet process pid
-define(COMET_PID, comet_pid).

%% Page state
%% Edited privileges is a proplist with SpaceID keys,
%% which values are proplists with UserID/GroupID keys, which values
%% are proplists with {PrivilegeID, Flag} tuples.
-record(page_state, {
    spaces = [],
    expanded_spaces = [],
    edited_group_privileges = [],
    edited_user_privileges = [],
    gruid,
    access_token}).

% Actions that can be performed by user, they do not require privileges.
% Theyare represented by tuples {action, ActionName, Args}
-define(ACTION_SHOW_CREATE_SPACE_POPUP, show_create_space_popup).
-define(ACTION_CREATE_SPACE, create_space).
-define(ACTION_SHOW_JOIN_SPACE_POPUP, show_join_space_popup).
-define(ACTION_JOIN_SPACE, join_space).
-define(ACTION_SHOW_LEAVE_SPACE_POPUP, show_leave_space_popup).
-define(ACTION_LEAVE_SPACE, leave_space).
-define(ACTION_MOVE_SPACE, move_space).
-define(ACTION_HIDE_POPUP, hide_popup).

%% Records used to store current info about spaces
%% current_privileges is a list of privileges of current user in specific space
-record(space_state, {users = [], groups = [], providers = [], current_privileges = []}).
-record(user_state, {id = <<"">>, name = <<"">>, privileges = <<"">>}).
-record(group_state, {id = <<"">>, name = <<"">>, privileges = <<"">>}).
-record(provider, {id = <<"">>, name = <<"">>, privileges = <<"">>}).


%% ====================================================================
%% API functions
%% ====================================================================

%% main/0
%% ====================================================================
%% @doc Template points to the template file, which will be filled with content.
-spec main() -> #dtl{}.
%% ====================================================================
main() ->
    case opn_gui_utils:maybe_redirect(true, false) of
        true ->
            #dtl{file = "bare", app = ?APP_Name, bindings = [
                {title, <<"">>},
                {body, <<"">>},
                {custom, <<"">>},
                {css, <<"">>}
            ]};
        false ->
            #dtl{file = "bare", app = ?APP_Name, bindings = [
                {title, title()},
                {body, body()},
                {custom, <<"">>},
                {css, css()}
            ]}
    end.


%% title/0
%% ====================================================================
%% @doc Page title.
-spec title() -> binary().
%% ====================================================================
title() -> <<"Spaces">>.


%% css/0
%% ====================================================================
%% @doc Page specific CSS import clause.
-spec css() -> binary().
%% ====================================================================
css() ->
    <<"<link rel=\"stylesheet\" href=\"/css/spaces.css\" type=\"text/css\" media=\"screen\" charset=\"utf-8\" />">>.


%% body/0
%% ====================================================================
%% @doc This will be placed instead of {{body}} tag in template.
-spec body() -> [#panel{}].
%% ====================================================================
body() ->
    [<<"DUPADUPA">>].


comet_loop_init(GRUID, AccessToken, ExpandedSpaces, ScrollToSpaceID) ->
    ok.

comet_loop(_) -> ok.


%% event/1
%% ====================================================================
%% @doc Handles page events.
-spec event(Event :: term()) -> no_return().
%% ====================================================================
event(init) ->
    try
        GRUID = utils:ensure_binary(opn_gui_utils:get_global_user_id()),
        AccessToken = opn_gui_utils:get_access_token(),
        {ExpandedSpaces, ScrollToSpaceID} =
            case gui_ctx:url_param(<<"show">>) of
                undefined -> {[], undefined};
                Bin -> {[Bin], Bin}
            end,

        gui_jq:wire(#api{name = "join_group", tag = "join_group"}, false),
        gui_jq:wire(#api{name = "leave_group", tag = "leave_group"}, false),
        gui_jq:bind_key_to_click_on_class(<<"13">>, <<"confirm">>),
        gui_jq:register_escape_event("escape_pressed_event"),

        {ok, Pid} = gui_comet:spawn(fun() -> comet_loop_init(GRUID, AccessToken, ExpandedSpaces, ScrollToSpaceID) end),
        put(?COMET_PID, Pid)
    catch
        _:Reason ->
            ?error_stacktrace("Cannot initialize page ~p: ~p", [?MODULE, Reason]),
            gui_jq:hide(<<"spinner">>),
            opn_gui_utils:message(error, <<"Cannot fetch spaces.<br />Please try again later.">>)
    end;

event({action, Action}) ->
    event({action, Action, []});

event({action, Action, Args}) ->
    ProcessedArgs = lists:map(
        fun(Arg) ->
            case Arg of
                {query_value, FieldName} ->
                    % This tuple means that element with id=FieldName has to be queried
                    % and the result be put in args
                    gui_ctx:postback_param(FieldName);
                Other ->
                    Other
            end
        end, Args),
    gui_jq:show(<<"spinner">>),
    get(?COMET_PID) ! {action, Action, ProcessedArgs};

event({group_action, Action, GroupID}) ->
    event({group_action, Action, GroupID, []});

event({group_action, Action, GroupID, Args}) ->
    ProcessedArgs = lists:map(
        fun(Arg) ->
            case Arg of
                {query_value, FieldName} ->
                    % This tuple means that element with id=FieldName has to be queried
                    % and the result be put in args
                    gui_ctx:postback_param(FieldName);
                Other ->
                    Other
            end
        end, Args),
    gui_jq:show(<<"spinner">>),
    get(?COMET_PID) ! {group_action, Action, GroupID, ProcessedArgs};

event({close_message, MessageId}) ->
    gui_jq:hide(MessageId);

event({redirect_to_group, SpaceID}) ->
    gui_jq:redirect(<<"/groups?show=", SpaceID/binary>>);

event(show_users_info) ->
    gui_jq:info_popup(<<"Users section">>,
        <<"This table shows all users that belong to the group and their privileges.<br /><br />",
        "- to modify privileges, set corresponding checkboxes and click the \"Save\" button<br />",
        "- to remove a user, point at the user and use the trash button<br />",
        "- to display user ID, point at the user icon<br />",
        "- to invite a user to the group, select the action from \"Actions\" menu.<br />">>, <<"">>);

event(show_groups_info) ->
    gui_jq:info_popup(<<"Spaces section">>,
        <<"This table shows all spaces to which the group belongs.<br /><br />",
        "- to leave a space, point at the space and use the exit button<br />",
        "- to see more details about a space, click on its name or icon<br />",
        "- to create a new space for the group, join an existing space or request space creation, select an action from \"Actions\" menu.<br />">>, <<"">>);

event(show_providers_info) ->
    gui_jq:info_popup(<<"Spaces section">>,
        <<"This table shows all spaces to which the group belongs.<br /><br />",
        "- to leave a space, point at the space and use the exit button<br />",
        "- to see more details about a space, click on its name or icon<br />",
        "- to create a new space for the group, join an existing space or request space creation, select an action from \"Actions\" menu.<br />">>, <<"">>);

event(terminate) ->
    ok.


%% api_event/3
%% ====================================================================
%% @doc Handles page events.
-spec api_event(Name :: string(), Args :: string(), Req :: string()) -> no_return().
%% ====================================================================
%% Handling events
api_event("escape_pressed_event", _, _) ->
    event({action, ?ACTION_HIDE_POPUP}).

