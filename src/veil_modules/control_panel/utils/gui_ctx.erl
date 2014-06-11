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

% Functions used to connect user to session
-export([set_user/2, get_user_id/0, get_user_record/0, clear/0]).


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
%% @doc Returns user database doc associated with current session.
%% @end
-spec clear() -> ok.
%% ====================================================================
clear() ->
    wf:user(undefined),
    wf:session(user_doc, undefined),
    wf:logout().

