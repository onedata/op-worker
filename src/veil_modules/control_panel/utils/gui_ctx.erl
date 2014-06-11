%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains functions correlated with page and session context.
%% @end
%% ===================================================================

-module(gui_ctx).
-include("veil_modules/control_panel/common.hrl").
-include("logging.hrl").

% Functions used to associate user with session
-export([set_user/2, get_user_id/0, get_user_record/0, user_logged_in/0, clear/0]).

% Functions connected with page / session context
-export([get_requested_hostname/0, get_requested_page/0, get_request_params/0]).

% Parameters querying
-export([param/1]).


%% ====================================================================
%% API functions
%% ====================================================================

%% set_user/2
%% ====================================================================
%% @doc Associates current session with a user. User ID and his
%% database doc must be provided.
%% @end
-spec set_user(ID :: term(), DBRecord :: term()) -> ok.
%% ====================================================================
set_user(ID, DBRecord) ->
    wf:user(ID),
    wf:session(user_doc, DBRecord).


%% set_user/0
%% ====================================================================
%% @doc Returns user ID associated with current session.
%% @end
-spec get_user_id() -> term().
%% ====================================================================
get_user_id() ->
    wf:user().


%% get_user_record/0
%% ====================================================================
%% @doc Returns user database doc associated with current session.
%% @end
-spec get_user_record() -> term().
%% ====================================================================
get_user_record() ->
    wf:session(user_doc).


%% clear/0
%% ====================================================================
%% @doc Clears the association between suer and session.
%% @end
-spec clear() -> ok.
%% ====================================================================
clear() ->
    wf:user(undefined),
    wf:session(user_doc, undefined),
    wf:logout().


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
    (gui_ctx:get_user_id() /= undefined).


%% param/1
%% ====================================================================
%% @doc Retrieves a parameter value for a given key. This can be both
%% URL parameter or POST parameter passed during form submission.
%% For form parameters, source field in event record must be provided
%% to be accessible by this function.
%% like this: #event{source = ["field_name"], ...}.
%% @end
-spec param(ParamName :: string() | binary()) -> ok.
%% ====================================================================
param(ParamName) ->
    wf:q(gui_str:to_list(ParamName)).