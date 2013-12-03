%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module gives high level DB API which contain veil cluster specific utility methods.
%% All DAO API functions should not be called directly. Call dao:handle(_, {cluster, MethodName, ListOfArgs) instead.
%% See {@link dao:handle/2} for more details.
%% @end
%% ===================================================================
-module(dao_cluster).

-include("veil_modules/dao/dao.hrl").
-include("veil_modules/dao/dao_types.hrl").
-include("veil_modules/dao/couch_db.hrl").

%% API - cluster state
-export([save_state/2, save_state/1, get_state/1, get_state/0, clear_state/1, clear_state/0]).

%% API - FUSE env variables
-export([get_fuse_env/1, save_fuse_env/1, remove_fuse_env/1]).

-ifdef(TEST).
-compile([export_all]).
-endif.

%% ===================================================================
%% API functions
%% ===================================================================

%% save_state/1
%% ====================================================================
%% @doc Saves cluster state Rec to DB with ID = cluster_state.
%% Should not be used directly, use {@link dao:handle/2} instead.
%% @end
-spec save_state(Rec :: tuple()) ->
    {ok, Id :: string()} |
    no_return(). % erlang:error(any()) | throw(any())
%% ====================================================================
save_state(Rec) when is_tuple(Rec) ->
    save_state(cluster_state, Rec).

%% save_state/2
%% ====================================================================
%% @doc Saves cluster state Rec to DB with ID = Id.
%% Should not be used directly, use {@link dao:handle/2} instead.
%% @end
-spec save_state(Id :: atom(), Rec :: tuple()) ->
    {ok, Id :: string()} |
    no_return(). % erlang:error(any()) | throw(any())
%% ====================================================================
save_state(Id, Rec) when is_tuple(Rec), is_atom(Id) ->
    dao:save_record(#veil_document{record = Rec, force_update = true, uuid = Id}).


%% get_state/0
%% ====================================================================
%% @doc Retrieves cluster state with ID = cluster_state from DB.
%% Should not be used directly, use {@link dao:handle/2} instead.
%% @end
-spec get_state() -> {ok, term()} | {error, any()}.
%% ====================================================================
get_state() ->
    get_state(cluster_state).


%% get_state/1
%% ====================================================================
%% @doc Retrieves cluster state with UUID = Id from DB.
%% Should not be used directly, use {@link dao:handle/2} instead.
%% @end
-spec get_state(Id :: atom()) -> {ok, term()} | {error, any()}.
%% ====================================================================
get_state(Id) ->
    case dao:get_record(Id) of
        {ok, State} ->
            {ok, State#veil_document.record};
        Other ->
            {error, Other}
    end.


%% clear_state/0
%% ====================================================================
%% @doc Removes cluster state with Id = cluster_state
%% Should not be used directly, use {@link dao:handle/2} instead.
%% @end
-spec clear_state() ->
    ok |
    no_return(). % erlang:error(any()) | throw(any())
%% ====================================================================
clear_state()->
    clear_state(cluster_state).


%% clear_state/2
%% ====================================================================
%% @doc Removes cluster state with given Id
%% Should not be used directly, use {@link dao:handle/2} instead.
%% @end
-spec clear_state(Id :: atom()) ->
    ok |
    no_return(). % erlang:error(any()) | throw(any())
%% ====================================================================
clear_state(Id) ->
    dao:remove_record(Id).


%% save_fuse_env/1
%% ====================================================================
%% @doc Saves fuse_env record to DB.
%% Should not be used directly, use {@link dao:handle/2} instead.
%% @end
-spec save_fuse_env(#fuse_env{} | #veil_document{}) ->
    {ok, Id :: string()} |
    no_return(). % erlang:error(any()) | throw(any())
%% ====================================================================
save_fuse_env(#fuse_env{} = Env) ->
    save_fuse_env(#veil_document{record = Env});
save_fuse_env(#veil_document{record = #fuse_env{}} = Doc) ->
    dao:save_record(Doc).


%% get_fuse_env/1
%% ====================================================================
%% @doc Gets fuse_env record with given FuseID form DB.
%% Should not be used directly, use {@link dao:handle/2} instead.
%% @end
-spec get_fuse_env(FuseId :: uuid()) ->
    {ok, #veil_document{}} |
    no_return(). % erlang:error(any()) | throw(any())
%% ====================================================================
get_fuse_env(FuseId) ->
    dao:get_record(FuseId).


%% remove_fuse_env/1
%% ====================================================================
%% @doc Removes fuse_env record with given FuseID form DB.
%% Should not be used directly, use {@link dao:handle/2} instead.
%% @end
-spec remove_fuse_env(FuseId :: uuid()) ->
    ok |
    no_return(). % erlang:error(any()) | throw(any())
%% ====================================================================
remove_fuse_env(FuseId) ->
    dao:remove_record(FuseId).

%% ===================================================================
%% Internal functions
%% ===================================================================
    
