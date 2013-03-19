%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module gives high level DB API which contain veil cluster specific utility methods.
%% All DAO API functions should not be called directly. Call dao:handle(_, {cluster, MethodName, ListOfArgs) instead.
%% See dao:handle/2 for more details.
%% @end
%% ===================================================================
-module(dao_cluster).

-include_lib("veil_modules/dao/dao.hrl").
-include_lib("veil_modules/dao/couch_db.hrl").

%% API
-export([save_state/2, get_state/1, clear_state/1]).

-ifdef(TEST).
-compile([export_all]).
-endif.

%% ===================================================================
%% API functions
%% ===================================================================

%% save_state/2
%% ====================================================================
%% @doc Saves cluster state Rec to DB with ID = Id.
%% Should not be used directly, use dao:handle/2 instead (See dao:handle/2 for more details).
%% @end
-spec save_state(Id :: atom(), Rec :: tuple()) ->
    ok |
    no_return(). % erlang:error(any()) | throw(any())
%% ====================================================================
save_state(Id, Rec) when is_tuple(Rec), is_atom(Id) ->
    dao:save_record(Id, Rec).

%% get_state/2
%% ====================================================================
%% @doc Retrieves cluster state with ID = Id from DB.
%% Should not be used directly, use dao:handle/2 instead (See dao:handle/2 for more details).
%% @end
-spec get_state(Id :: atom()) -> not_yet_implemented.
%% ====================================================================
get_state(_Id) ->
    %% Below we have full implementation of this method except of record initialization (see TODO)
    %% StateRecord = #some_record{}, %% TODO: This record should be replaced with a real cluster state record (which at this moment is not implemented)
    %% dao:get_record(Id, StateRecord).
    not_yet_implemented.


%% clear_state/2
%% ====================================================================
%% @doc Removes cluster state with given Id
%% Should not be used directly, use dao:handle/2 instead (See dao:handle/2 for more details).
%% @end
-spec clear_state(Id :: atom()) ->
    ok |
    no_return(). % erlang:error(any()) | throw(any())
%% ====================================================================
clear_state(_Id) ->
    %% Below we have full implementation of this method except of record initialization (see TODO)
    %% RecName = element(1, #some_record{}), %% TODO: This record should be replaced with a real cluster state record (which at this moment is not implemented)
    %% dao:remove_record(Id, RecName).
    not_yet_implemented.


%% ===================================================================
%% Internal functions
%% ===================================================================
    
