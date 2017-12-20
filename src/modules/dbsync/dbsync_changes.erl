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

-include("modules/datastore/datastore_models.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_links.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([apply_batch/2, apply/1]).

-type ctx() :: datastore_cache:ctx().
-type key() :: datastore:key().
-type doc() :: datastore:doc().
-type model() :: datastore_model:model().

%% Time to wait for worker process
-define(WORKER_TIMEOUT, 90000).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Applies remote changes.
%% @end
%%--------------------------------------------------------------------
-spec apply_batch([datastore:doc()], {couchbase_changes:since(),
    couchbase_changes:until()}) -> ok.
apply_batch(Docs, BatchRange) ->
    Master = self(),
    spawn_link(fun() ->
        DocsGroups = group_changes(Docs),
        DocsList = maps:to_list(DocsGroups),
        Ref = make_ref(),
        parallel_apply(DocsList, Ref),
        Ans = gather_answers(DocsList, Ref),
        Master ! {batch_applied, BatchRange, Ans}
    end),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Applies remote changes.
%% @end
%%--------------------------------------------------------------------
-spec apply(datastore:doc()) -> ok | {error, datastore:seq(), term()}.
apply(Doc = #document{value = Value, scope = SpaceId, seq = Seq}) ->
    try
        case Value of
            #links_forest{model = Model, key = Key} ->
                links_save(Model, Key, Doc);
            #links_node{model = Model, key = Key} ->
                links_save(Model, Key, Doc);
            #links_mask{} ->
                links_delete(Doc);
            _ ->
                Model = element(1, Value),
                Ctx = datastore_model_default:get_ctx(Model),
                Ctx2 = Ctx#{sync_change => true, hooks_disabled => true},
                {ok, _} = datastore_model:save(Ctx2, Doc)
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
%% Saves datastore links document.
%% @end
%%--------------------------------------------------------------------
-spec links_save(model(), key(), doc()) -> ok.
links_save(Model, RoutingKey, Doc = #document{key = Key}) ->
    Ctx = datastore_model_default:get_ctx(Model),
    Ctx2 = Ctx#{
        sync_change => true,
        local_links_tree_id => oneprovider:get_provider_id()
    },
    Ctx3 = datastore_multiplier:extend_name(RoutingKey, Ctx2),
    {ok, _} = datastore_router:route(Ctx3, RoutingKey, save, [
        Ctx3, Key, Doc
    ]),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes datastore links based on links mask.
%% @end
%%--------------------------------------------------------------------
-spec links_delete(doc()) -> ok.
links_delete(Doc = #document{key = Key, value = LinksMask = #links_mask{
    model = Model, tree_id = TreeId
}, deleted = false}) ->
    LocalTreeId = oneprovider:get_provider_id(),
    case TreeId of
        LocalTreeId ->
            Ctx = datastore_model_default:get_ctx(Model),
            Ctx2 = Ctx#{
                sync_change => true,
                local_links_tree_id => LocalTreeId
            },
            Ctx3 = datastore_multiplier:extend_name(Key, Ctx2),
            DeletedLinks = get_links_mask(Ctx3, Key),
            Deleted = apply_links_mask(Ctx3, LinksMask, DeletedLinks),
            save_links_mask(Ctx, Doc#document{deleted = Deleted});
        _ ->
            ok
    end;
links_delete(Doc = #document{
    key = Key,
    mutators = [TreeId, RemoteTreeId],
    value = #links_mask{model = Model, tree_id = RemoteTreeId},
    deleted = true
}) ->
    LocalTreeId = oneprovider:get_provider_id(),
    case TreeId of
        LocalTreeId ->
            Ctx = datastore_model_default:get_ctx(Model),
            Ctx2 = Ctx#{
                sync_change => true,
                local_links_tree_id => LocalTreeId
            },
            Ctx3 = datastore_multiplier:extend_name(Key, Ctx2),
            save_links_mask(Ctx3, Doc);
        _ ->
            ok
    end;
links_delete(#document{}) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns list of masked links that have been already deleted.
%% @end
%%--------------------------------------------------------------------
-spec get_links_mask(ctx(), key()) -> [links_mask:link()].
get_links_mask(Ctx, Key) ->
    case datastore_router:route(Ctx, Key, get, [Ctx, Key]) of
        {ok, #document{value = #links_mask{links = Links}}} -> Links;
        {error, not_found} -> []
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Applies links mask by deleting masked links.
%% @end
%%--------------------------------------------------------------------
-spec apply_links_mask(ctx(), links_mask:mask(), [links_mask:link()]) ->
    boolean().
apply_links_mask(Ctx, #links_mask{key = Key, tree_id = TreeId, links = Links},
    DeletedLinks) ->
    LinksSet = gb_sets:from_list(Links),
    DeletedLinksSet = gb_sets:from_list(DeletedLinks),
    Links2 = gb_sets:to_list(gb_sets:subtract(LinksSet, DeletedLinksSet)),
    Results = datastore_router:route(Ctx, Key, delete_links, [
        Ctx, Key, TreeId, Links2
    ]),
    true = lists:all(fun(Result) -> Result == ok end, Results),
    Size = application:get_env(cluster_worker, datastore_links_mask_size, 1000),
    erlang:length(Links) == Size.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Saves datastore links mask.
%% @end
%%--------------------------------------------------------------------
-spec save_links_mask(ctx(), doc()) -> ok.
save_links_mask(Ctx, Doc = #document{key = Key}) ->
    {ok, _} = datastore_router:route(Ctx, Key, save, [
        Ctx, Key, Doc
    ]),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns key connected with particular change.
%% @end
%%--------------------------------------------------------------------
-spec get_change_key(datastore:doc()) -> datastore:key().
get_change_key(#document{value = #file_location{uuid = FileUuid}}) ->
    FileUuid;
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
%% Starts one worker for each documents' group.
%% @end
%%--------------------------------------------------------------------
-spec parallel_apply([{datastore:key(),
    [datastore:doc()]}], reference()) -> ok.
parallel_apply(DocsList, Ref) ->
    Master = self(),
    lists:foreach(fun({_, DocList}) ->
        spawn(fun() ->
            SlaveAns = lists:foldl(fun
                (Doc, ok) ->
                    dbsync_changes:apply(Doc);
                (_, Acc) ->
                    Acc
            end, ok, lists:reverse(DocList)),
            Master ! {changes_worker_ans, Ref, SlaveAns}
        end)
    end, DocsList).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gather answers from workers.
%% @end
%%--------------------------------------------------------------------
-spec gather_answers(list(), reference()) ->
    ok | timeout | {error, datastore:seq(), term()}.
gather_answers(SlavesList, Ref) ->
    gather_answers(length(SlavesList), Ref, ok).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gather appropriate number of workers' answers.
%% @end
%%--------------------------------------------------------------------
-spec gather_answers(non_neg_integer(), reference(),
    ok | {error, datastore:seq(), term()}) ->
    ok | timeout | {error, datastore:seq(), term()}.
gather_answers(0, _Ref, Ans) ->
    Ans;
gather_answers(N, Ref, TmpAns) ->
    receive
        {changes_worker_ans, Ref, Ans} ->
            Merged = case {Ans, TmpAns} of
                {ok, _} -> TmpAns;
                {{error, Seq, _}, {error, Seq2, _}} when Seq < Seq2 -> Ans;
                {{error, _, _}, {error, _, _}} -> TmpAns;
                _ -> Ans
            end,
            gather_answers(N - 1, Ref, Merged)
    after
        ?WORKER_TIMEOUT -> timeout
    end.