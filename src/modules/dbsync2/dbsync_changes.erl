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

-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_common_internal.hrl").

%% API
-export([apply_batch/1, apply/1]).

%% Time to wait for slave process
-define(SLAVE_TIMEOUT, 30000).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Applies remote changes.
%% @end
%%--------------------------------------------------------------------
-spec apply_batch([datastore:doc()]) -> ok | timeout | {error, datastore:seq(), term()}.
apply_batch(Docs) ->
    DocsGroups = group_changes(Docs),
    DocsList = maps:to_list(DocsGroups),
    start_slaves(DocsList),
    gather_answers(DocsList).

%%--------------------------------------------------------------------
%% @doc
%% Applies remote changes.
%% @end
%%--------------------------------------------------------------------
-spec apply(datastore:doc()) -> ok | {error, datastore:seq(), term()}.
% TODO - do we sync any listed models?
apply(Doc = #document{key = Key, value = Value, scope = SpaceId, seq = Seq}) ->
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

        try
            dbsync_events:change_replicated(SpaceId, Doc)
        catch
            _:Reason_ ->
                ?error_stacktrace("Change ~p post-processing failed due "
                "to: ~p", [Doc, Reason_])
        end,
        ok
    catch
        _:Reason ->
            ?error_stacktrace("Unable to apply change ~p due to: ~p",
                [Doc, Reason]),
            {error, Seq, Reason}
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
        [Key], [{hooks_config, no_hooks}, {links_tree, {true, MainDocKey}}]).


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
    Result = model:execute_with_default_context(ModelConfig, save,
        [Doc#document{seq = null}], [{hooks_config, no_hooks},
            {resolve_conflicts, true}, {links_tree, {true, MainDocKey}},
            {disc_driver_ctx, no_seq, true}
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

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns key connected with particular change.
%% @end
%%--------------------------------------------------------------------
-spec get_change_key(datastore:doc()) -> datastore:ext_key().
get_change_key(#document{value = #file_location{uuid = FileUuid}}) ->
    FileUuid;
get_change_key(#document{value = #links{doc_key = DocUuid}}) ->
    DocUuid;
get_change_key(#document{key = Uuid}) ->
    Uuid.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Group changes - documents connected with single file are grouped together.
%% @end
%%--------------------------------------------------------------------
-spec group_changes([datastore:doc()]) -> #{}.
group_changes(Docs) ->
    lists:foldl(fun(Doc, Acc) ->
        ChangeKey = get_change_key(Doc),
        ChangeList = maps:get(ChangeKey, Acc, []),
        maps:put(ChangeKey, [Doc | ChangeList], Acc)
    end, #{}, Docs).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Starts one slave for each documents' group.
%% @end
%%--------------------------------------------------------------------
-spec start_slaves([{datastore:ext_key(), [datastore:doc()]}]) -> ok.
start_slaves(DocsList) ->
    Master = self(),
    lists:foreach(fun({_, DocList}) ->
        spawn(fun() ->
            SlaveAns = lists:foldl(fun
                (Doc, ok) ->
                    apply(Doc);
                (_, Acc) ->
                    Acc
            end, ok, lists:reverse(DocList)),
            Master ! {changes_slave_ans, SlaveAns}
        end)
    end, DocsList).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gather answers from slaves.
%% @end
%%--------------------------------------------------------------------
-spec gather_answers(list()) -> ok | timeout | {error, datastore:seq(), term()}.
gather_answers(SlavesList) ->
    gather_answers(length(SlavesList), ok).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gather appropriate number of slaves' answers.
%% @end
%%--------------------------------------------------------------------
-spec gather_answers(non_neg_integer(), ok | {error, datastore:seq(), term()}) ->
    ok | timeout | {error, datastore:seq(), term()}.
gather_answers(0, TmpAns) ->
    TmpAns;
gather_answers(N, TmpAns) ->
    receive
        {changes_slave_ans, Ans} ->
            Merged = case {Ans, TmpAns} of
                {ok, _} -> TmpAns;
                {{error, Seq, _}, {error, Seq2, _}} when Seq < Seq2 -> Ans;
                {{error, _, _}, {error, _, _}} -> TmpAns;
                _ -> Ans
            end,
            gather_answers(N - 1, Merged)
    after
        ?SLAVE_TIMEOUT -> timeout
    end.