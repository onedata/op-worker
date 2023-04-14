%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for application of remote changes.
%%% @end
%%%-------------------------------------------------------------------
-module(dbsync_changes).
-author("Krzysztof Trzepla").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_links.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([apply_batch/5, apply/1, get_ctx/2]).

-type ctx() :: datastore_cache:ctx().
-type key() :: datastore:key().
-type doc() :: datastore:doc().
-type model() :: datastore_model:model().
-type timestamp() :: datastore_doc:timestamp() | undefined.

-export_type([timestamp/0]).

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
-spec apply_batch([datastore:doc()], {couchbase_changes:since(), couchbase_changes:until()},
    timestamp(), od_space:id(), od_provider:id()) -> ok.
apply_batch([], BatchRange, Timestamp, _SpaceId, _ProviderId) ->
    % Empty batch (all sequences have been overwritten by newer changes)
    self() ! {batch_applied, BatchRange, Timestamp, ok},
    ok;
apply_batch(Docs, BatchRange, Timestamp, SpaceId, ProviderId) ->
    Master = self(),
    spawn_link(fun() ->
        DocsGroups = group_changes(Docs),
        DocsList = maps:values(DocsGroups),

        MinSize = length(Docs) / op_worker:get_env(dbsync_changes_max_procs, 50),
        {LastGroup, DocsList2} = lists:foldl(fun(Group, {CurrentGroup, Acc}) ->
            Group2 = Group ++ CurrentGroup,
            case length(Group2) >= MinSize of
                true ->
                    {[], [Group2 | Acc]};
                _ ->
                    {Group2, Acc}
            end
        end, {[], []}, DocsList),
        DocsList3 = case LastGroup of
            [] -> DocsList2;
            _ -> [LastGroup | DocsList2]
        end,

        Ref = make_ref(),
        Pids = parallel_apply(DocsList3, Ref),
        Ans = gather_answers(Pids, Ref),
        dbsync_logger:log_apply(Docs, BatchRange, Ans, SpaceId, ProviderId),
        Master ! {batch_applied, BatchRange, Timestamp, Ans}
    end),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Applies remote changes.
%% @end
%%--------------------------------------------------------------------
-spec apply(datastore:doc()) -> ok | {error, datastore_doc:seq(), term()}.
apply(Doc = #document{value = Value, scope = SpaceId, seq = Seq}) ->
    try
        DocToHandle = case Value of
            #links_forest{model = Model, key = Key} ->
                links_save(Model, Key, Doc);
            #links_node{model = Model, key = Key} ->
                links_save(Model, Key, Doc);
            #links_mask{} ->
                links_delete(Doc);
            _ ->
                Model = element(1, Value),
                Ctx = get_ctx(Model, Doc),
                Ctx2 = Ctx#{sync_change => true, hooks_disabled => true},
                case datastore_model:save(Ctx2, Doc) of
                    {ok, Doc2} ->
                        case Value of
                            #file_location{} ->
                                fslogic_location_cache:cache_location(Doc2);
                            _ ->
                                ok
                        end,
                        Doc2;
                    {error, already_exists} ->
                        Doc;
                    {error, ignored} ->
                        undefined
                end
        end,

        try
            dbsync_events:change_replicated(SpaceId, DocToHandle)
        catch
            Class:Reason:Stacktrace ->
                ?error_exception("when post-processing change:~s", [?autoformat([Doc])], Class, Reason, Stacktrace)
        end,
        ok
    catch
        Class2:Reason2:Stacktrace2 ->
            ?error_exception(?autoformat([Doc]), Class2, Reason2, Stacktrace2),
            {error, Seq, Reason2}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Saves datastore links document and returns doc used for applying changes or
%% `undefined` if remote doc was ignored.
%% @end
%%--------------------------------------------------------------------
-spec links_save(model(), key(), doc()) -> undefined | doc().
links_save(Model, RoutingKey, Doc = #document{key = Key}) ->
    Ctx = get_ctx(Model, Doc),
    Ctx2 = Ctx#{
        sync_change => true,
        local_links_tree_id => oneprovider:get_id(),
        routing_key => RoutingKey
    },
    Ctx3 = datastore_multiplier:extend_name(RoutingKey, Ctx2),
    case datastore_router:route(save, [Ctx3, Key, Doc]) of
        {ok, Doc2} ->
            Doc2;
        {error, already_exists} ->
            Doc;
        {error, ignored} ->
            undefined
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes datastore links based on links mask and returns doc used for
%% applying changes.
%% @end
%%--------------------------------------------------------------------
-spec links_delete(doc()) -> undefined | doc().
links_delete(Doc = #document{key = Key, value = LinksMask = #links_mask{
    key = RoutingKey, model = Model, tree_id = TreeId
}, deleted = false}) ->
    LocalTreeId = oneprovider:get_id(),
    case TreeId of
        LocalTreeId ->
            Ctx = get_ctx(Model, Doc),
            Ctx2 = Ctx#{
                sync_change => true,
                local_links_tree_id => LocalTreeId
            },
            Ctx3 = datastore_multiplier:extend_name(RoutingKey, Ctx2),
            DeletedLinks = get_links_mask(Ctx3, Key, RoutingKey),
            Deleted = apply_links_mask(Ctx3, LinksMask, DeletedLinks),
            Ctx2_2 = datastore_multiplier:extend_name(RoutingKey, Ctx),
            save_links_mask(Ctx2_2, Doc#document{deleted = Deleted});
        _ ->
            undefined
    end;
links_delete(Doc = #document{
    mutators = [TreeId, RemoteTreeId],
    value = #links_mask{key = RoutingKey, model = Model, tree_id = RemoteTreeId},
    deleted = true
}) ->
    LocalTreeId = oneprovider:get_id(),
    case TreeId of
        LocalTreeId ->
            Ctx = get_ctx(Model, Doc),
            Ctx2 = Ctx#{
                sync_change => true,
                local_links_tree_id => LocalTreeId
            },
            Ctx3 = datastore_multiplier:extend_name(RoutingKey, Ctx2),
            save_links_mask(Ctx3, Doc);
        _ ->
            undefined
    end;
links_delete(#document{}) ->
    undefined.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns list of masked links that have been already deleted.
%% @end
%%--------------------------------------------------------------------
-spec get_links_mask(ctx(), key(), key()) -> [links_mask:link()].
get_links_mask(Ctx, Key, RoutingKey) ->
    case datastore_router:route(get, [Ctx#{routing_key => RoutingKey}, Key]) of
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
    Results = datastore_router:route(delete_links, [
        Ctx#{routing_key => Key}, Key, TreeId, Links2
    ]),

    Check = lists:all(fun(Result) -> Result == ok end, Results),
    case Check of
        true ->
            ok;
        _ ->
            ?error("apply_links_mask error: ~p for args: ~p",
                [Results, {Ctx, Key, TreeId, Links2}])
    end,

    true = Check,
    Size = application:get_env(cluster_worker, datastore_links_mask_size, 1000),
    erlang:length(Links) == Size.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Saves datastore links mask and returns doc used for applying changes or
%% `undefined` if remote doc was ignored.
%% @end
%%--------------------------------------------------------------------
-spec save_links_mask(ctx(), doc()) -> undefined | doc().
save_links_mask(Ctx, Doc = #document{key = Key,
    value = #links_mask{key = RoutingKey}}) ->
    case datastore_router:route(save, [Ctx#{routing_key => RoutingKey}, Key, Doc]) of
        {ok, Doc2} ->
            Doc2;
        {error, already_exists} ->
            Doc;
        {error, ignored} ->
            undefined
    end.

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
-spec group_changes([datastore:doc()]) -> map().
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
-spec parallel_apply([[datastore:doc()]], reference()) -> [pid()].
parallel_apply(DocsList, Ref) ->
    Master = self(),
    lists:map(fun(DocList) ->
        spawn(fun() ->
            SlaveAns = lists:foldl(fun
                (Doc, ok) ->
                    dbsync_changes:apply(Doc);
                (#document{seq = DocSeq} = Doc, {error, ErrorSeq, _} = Acc) when DocSeq < ErrorSeq ->
                    case dbsync_changes:apply(Doc) of
                        ok -> Acc;
                        {error, _, _} = ApplyError -> ApplyError
                    end;
                (_, Acc) ->
                    Acc
            end, ok, lists:reverse(DocList)),
            Master ! {changes_worker_ans, Ref, self(), SlaveAns}
        end)
    end, DocsList).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gather answers from workers.
%% @end
%%--------------------------------------------------------------------
-spec gather_answers([pid()], reference()) ->
    ok | timeout | {error, datastore_doc:seq(), term()}.
gather_answers(SlavesList, Ref) ->
    gather_answers(SlavesList, Ref, ok, false).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gather appropriate number of workers' answers.
%% @end
%%--------------------------------------------------------------------
-spec gather_answers([pid()], reference(),
    ok | {error, datastore_doc:seq(), term()}, boolean()) ->
    ok | timeout | {error, datastore_doc:seq(), term()}.
gather_answers([], _Ref, Ans, _FinalCheck) ->
    Ans;
gather_answers(Pids, Ref, TmpAns, FinalCheck) ->
    receive
        {changes_worker_ans, Ref, Pid, Ans} ->
            Merged = case {Ans, TmpAns} of
                {ok, _} -> TmpAns;
                {{error, Seq, _}, {error, Seq2, _}} when Seq < Seq2 -> Ans;
                {{error, _, _}, {error, _, _}} -> TmpAns;
                _ -> Ans
            end,
            gather_answers(Pids -- [Pid], Ref, Merged, FinalCheck)
    after
        ?WORKER_TIMEOUT ->
            IsAnyAlive = lists:foldl(fun
                (_, true) ->
                    true;
                (Pid, _Acc) ->
                    erlang:is_process_alive(Pid)
            end, false, Pids),
            case {FinalCheck, IsAnyAlive} of
                {false, true} ->
                    gather_answers(Pids, Ref, TmpAns, FinalCheck);
                {false, false} ->
                    gather_answers(Pids, Ref, TmpAns, true);
                _ ->
                    timeout
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns model context used during application of changes.
%% Warning: if any traverse callback module uses other sync info than one provided by tree_traverse, this function
%% has to be extended to parse #document and get callback module.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx(model(), datastore:doc()) -> ctx().
get_ctx(Model, Doc) ->
    Ctx = case Model of
        traverse_task ->
            Ctx0 = tree_traverse:get_sync_info(),
            datastore_model_default:set_defaults(Ctx0#{model => Model});
        _ ->
            datastore_model_default:get_ctx(Model)
    end,

    case Doc of
        #document{deleted = true} -> datastore_model:ensure_expiry_set_on_delete(Ctx);
        _ -> Ctx
    end.