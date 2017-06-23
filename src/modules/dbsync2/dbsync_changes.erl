%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for application of remote changes.
%%% @end
%%%-------------------------------------------------------------------
-module(dbsync_changes).
-author("Krzysztof Trzepla").

-include_lib("ctool/include/logging.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_common_internal.hrl").

%% API
-export([apply/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Applies remote changes.
%% @end
%%--------------------------------------------------------------------
-spec apply(datastore:doc()) -> any().
% TODO - do we sync any listed models?
apply(Doc = #document{key = Key, value = Value, scope = SpaceId}) ->
    try
        case Value of
            #links{origin = Origin, doc_key = MainDocKey, model = ModelName} ->
                ModelConfig = ModelName:model_init(),

                OldLinks = case foreign_links_get(ModelConfig, Key, MainDocKey) of
                    {ok, #document{value = OldLinks1}} ->
                        OldLinks1;
                    {error, _Reason0} ->
                        #links{link_map = #{}, model = ModelName}
                end,

                CurrentLinks = #links{} = foreign_links_save(ModelConfig, Doc),
                {AddedMap, DeletedMap} = links_utils:diff(OldLinks, CurrentLinks),
                ok = dbsync_events:links_changed(
                    Origin, ModelName, MainDocKey, AddedMap, DeletedMap
                );
            _ ->
                ModelName = element(1, Value),
                ModelConfig = ModelName:model_init(),
                ok = model:execute_with_default_context(ModelConfig, save,
                    [Doc], [{hooks_config, no_hooks}, {resolve_conflicts, true}])
        end,

        Master = self(),
        spawn(fun() ->
            try
                dbsync_events:change_replicated(SpaceId, Doc, Master),
                Master ! {change_replicated_ok, Key}
            catch
                _:Reason ->
                    ?error_stacktrace("Change ~p post-processing failed due "
                    "to: ~p", [Doc, Reason]),
                    Master ! {change_replication_error, Key}
            end
        end),
        receive
            {change_replicated_ok, Key} -> ok;
            {file_consistency_wait, Key} -> ok;
            {change_replication_error, Key} -> ok
        after
            500 -> ok
        end
    catch
        _:Reason ->
            ?error_stacktrace("Unable to apply change ~p due to: ~p",
                [Doc, Reason]),
            {error, Reason}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets current version of links' document.
%% @end
%%--------------------------------------------------------------------
-spec foreign_links_get(model_behaviour:model_config(),
    datastore:ext_key(), datastore:ext_key()) ->
    {ok, datastore:document()} | {error, Reason :: any()}.
foreign_links_get(ModelConfig, Key, MainDocKey) ->
    model:execute_with_default_context(ModelConfig, get,
        [Key], [{hooks_config, no_hooks}, {links_tree, {true, MainDocKey}},
            {disc_driver_ctx, bucket, <<"default">>}]).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Saves links document received from other provider and returns current version
%% of given document.
%% @end
%%--------------------------------------------------------------------
-spec foreign_links_save(model_behaviour:model_config(), datastore:document()) ->
    #links{} | {error, Reason :: any()}.
foreign_links_save(ModelConfig, Doc = #document{key = Key, value = #links{
    doc_key = MainDocKey
} = Links}) ->
    Result = model:execute_with_default_context(ModelConfig, save, [Doc], [
        {hooks_config, no_hooks}, {resolve_conflicts, true},
        {links_tree, {true, MainDocKey}}, {disc_driver_ctx, bucket, <<"default">>}
    ]),
    case Result of
        ok ->
            case foreign_links_get(ModelConfig, Key, MainDocKey) of
                {error, {not_found, _}} ->
                    Links#links{link_map = #{}, children = #{}};
                {ok, #document{value = CurrentLinks}} ->
                    CurrentLinks
            end;
        Error ->
            ?error("Unable to save forign links document ~p due to ~p",
                [Doc, Error]),
            Error
    end.