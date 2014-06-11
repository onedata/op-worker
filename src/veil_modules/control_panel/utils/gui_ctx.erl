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
-export([set_user_id/1, get_user_id/0, set_user_record/1, get_user_record/0, user_logged_in/0, clear_session/0]).

% Functions connected with page / session context
-export([get_requested_hostname/0, get_requested_page/0, get_request_params/0]).

% Parameters querying
-export([form_param/1, url_param/1]).


%% ====================================================================
%% API functions
%% ====================================================================

%% set_user_id/1
%% ====================================================================
%% @doc Associates current session with a user. User ID and his
%% database doc must be provided.
%% @end
-spec set_user_id(ID :: term()) -> ok.
%% ====================================================================
set_user_id(ID) ->
    wf:user(ID).


%% get_user_id/0
%% ====================================================================
%% @doc Returns user ID associated with current session.
%% @end
-spec get_user_id() -> term().
%% ====================================================================
get_user_id() ->
    wf:user().


%% set_user_record/1
%% ====================================================================
%% @doc Returns user database doc associated with current session.
%% @end
-spec set_user_record(UserDoc :: term()) -> term().
%% ====================================================================
set_user_record(UserDoc) ->
    wf:session(user_doc, UserDoc).


%% get_user_record/0
%% ====================================================================
%% @doc Returns user database doc associated with current session.
%% @end
-spec get_user_record() -> term().
%% ====================================================================
get_user_record() ->
    wf:session(user_doc).


%% clear_session/0
%% ====================================================================
%% @doc Clears the association between suer and session.
%% @end
-spec clear_session() -> ok.
%% ====================================================================
clear_session() ->
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


%% form_param/1
%% ====================================================================
%% @doc Retrieves a parameter value for a given key - POST parameter
%% passed during form submission.
%% NOTE! The submit button must be wired in certain way
%% for the param to be accessible by this function,
%% like this: #button { actions = gui_jq:form_submit_action(...) }
%% Returns undefined if
%% the key is not found.
%% @end
-spec form_param(ParamName :: string() | binary()) -> binary() | undefined.
%% ====================================================================
form_param(ParamName) ->
    gui_str:to_binary(wf:q(gui_str:to_list(ParamName))).


%% url_param/1
%% ====================================================================
%% @doc Retrieves a URL parameter for given key. Returns undefined if
%% the key is not found.
%% @end
-spec url_param(ParamName :: string() | binary()) -> binary() | undefined.
%% ====================================================================
url_param(ParamName) ->
    wf:q(gui_str:to_binary(ParamName)).