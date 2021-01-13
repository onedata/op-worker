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
-include("modules/dbsync/dbsync.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_links.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([apply_batch/4, apply/2]).

-type ctx() :: datastore_cache:ctx().
-type key() :: datastore:key().
-type doc() :: datastore:doc().
-type remote_mutation_info() :: datastore_doc:remote_mutation_info().
-type model() :: datastore_model:model().
-type timestamp() :: datastore_doc:timestamp() | undefined.
-type dbsync_successful_application_result() :: #dbsync_application_result{}.
% Extend dbsync_successful_application_result() type in case of timeout
% (dbsync_successful_application_result record cannot be created).
-type dbsync_application_result() :: dbsync_successful_application_result() | timeout.

-export_type([remote_mutation_info/0, timestamp/0, dbsync_application_result/0]).

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
    couchbase_changes:until()}, timestamp(), oneprovider:id()) -> ok.
apply_batch(Docs, BatchRange, Timestamp, ProviderId) ->
    Master = self(),
    spawn_link(fun() ->
        DocsGroups = group_changes(Docs),
        DocsList = maps:values(DocsGroups),

        MinSize = application:get_env(?APP_NAME,
            dbsync_changes_apply_min_group_size, 10),

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
        Pids = parallel_apply(DocsList3, Ref, ProviderId),
        Ans = gather_answers(Pids, Ref),
        Master ! {batch_applied, BatchRange, Timestamp, Ans}
    end),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Applies remote changes. In case of success returns remote_mutation_info()
%% or undefined if document is ignored.
%% @end
%%--------------------------------------------------------------------
-spec apply(datastore:doc(), oneprovider:id()) ->
    {ok | ?REMOTE_DOC_ALREADY_EXISTS, remote_mutation_info() | undefined} | {error, datastore_doc:remote_seq()}.
apply(Doc = #document{value = Value, scope = SpaceId, seq = Seq}, ProviderId) ->
    try
        HandlingAns = case Value of
            #links_forest{model = Model, key = Key} ->
                links_save(Model, Key, Doc, ProviderId);
            #links_node{model = Model, key = Key} ->
                links_save(Model, Key, Doc, ProviderId);
            #links_mask{} ->
                links_delete(Doc, ProviderId);
            _ ->
                Model = element(1, Value),
                Ctx = (get_ctx(Model))#{hooks_disabled => true},
                case datastore_model:save_remote(Ctx, Doc, ProviderId) of
                    {ok, Doc2, _} = OkAns ->
                        case Value of
                            #file_location{} ->
                                fslogic_location_cache:cache_location(Doc2);
                            _ ->
                                ok
                        end,
                        OkAns;
                    Other ->
                        Other
                end
        end,

        case HandlingAns of
            {ok, DocToHandle, RemoteUpdateDesc} ->
                try
                    dbsync_events:change_replicated(SpaceId, DocToHandle)
                catch
                    _:Reason_ ->
                        ?error_stacktrace("Change ~p post-processing failed due "
                        "to: ~p", [Doc, Reason_])
                end,

                {ok, RemoteUpdateDesc};
            {error, ignored} ->
                {ok, undefined};
            {error, {?REMOTE_DOC_ALREADY_EXISTS, _} = ErrorDesc} ->
                ErrorDesc
        end
    catch
        _:Reason ->
            ?error_stacktrace("Unable to apply change ~p due to: ~p",
                [Doc, Reason]),
            {error, Seq}
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
-spec links_save(model(), key(), doc(), oneprovider:id()) ->
    {ok, doc(), remote_mutation_info()} | {error, ignored}.
links_save(Model, RoutingKey, Doc = #document{key = Key}, ProviderId) ->
    Ctx = get_ctx(Model),
    Ctx2 = Ctx#{
        local_links_tree_id => oneprovider:get_id(),
        routing_key => RoutingKey
    },
    Ctx3 = datastore_multiplier:extend_name(RoutingKey, Ctx2),
    datastore_router:route(save_remote, [Ctx3, Key, Doc, ProviderId]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes datastore links based on links mask and returns doc used for
%% applying changes.
%% @end
%%--------------------------------------------------------------------
-spec links_delete(doc(), oneprovider:id()) -> {ok, doc(), remote_mutation_info()} | {error, ignored}.
links_delete(Doc = #document{key = Key, value = LinksMask = #links_mask{
    key = RoutingKey, model = Model, tree_id = TreeId
}, deleted = false}, ProviderId) ->
    LocalTreeId = oneprovider:get_id(),
    case TreeId of
        LocalTreeId ->
            Ctx = (get_ctx(Model))#{local_links_tree_id => LocalTreeId},
            Ctx2 = datastore_multiplier:extend_name(RoutingKey, Ctx),
            DeletedLinks = get_links_mask(Ctx2, Key, RoutingKey),
            IsMaskFull = apply_links_mask(Ctx2, LinksMask, DeletedLinks),
            Ans = save_links_mask(Ctx2, Doc, ProviderId),

            % Mask document is full - delete it
            case IsMaskFull of
                true ->
                    DocToDelete = case Ans of
                        {ok, SavedDoc, _RemoteUpdateDesc} -> SavedDoc;
                        {error, ignored} -> Doc
                    end,
                    {ok, _} = datastore_router:route(save,
                        [Ctx2#{routing_key => RoutingKey}, Key, DocToDelete#document{deleted = IsMaskFull}]);
                false ->
                    ok
            end,

            Ans;
        _ ->
            {error, ignored}
    end;
links_delete(Doc = #document{
    mutators = [TreeId, RemoteTreeId],
    value = #links_mask{key = RoutingKey, model = Model, tree_id = RemoteTreeId},
    deleted = true
}, ProviderId) ->
    LocalTreeId = oneprovider:get_id(),
    case TreeId of
        LocalTreeId ->
            Ctx = (get_ctx(Model))#{local_links_tree_id => LocalTreeId},
            Ctx2 = datastore_multiplier:extend_name(RoutingKey, Ctx),
            save_links_mask(Ctx2, Doc, ProviderId);
        _ ->
            {error, ignored}
    end;
links_delete(#document{}, _ProviderId) ->
    {error, ignored}.

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
-spec save_links_mask(ctx(), doc(), oneprovider:id()) -> {ok, doc(), remote_mutation_info()} | {error, ignored}.
save_links_mask(Ctx, Doc = #document{key = Key,
    value = #links_mask{key = RoutingKey}}, ProviderId) ->
    datastore_router:route(save_remote, [Ctx#{routing_key => RoutingKey}, Key, Doc, ProviderId]).

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
%% Starts one worker for each documents' list.
%% @end
%%--------------------------------------------------------------------
-spec parallel_apply([[datastore:doc()]], reference(), oneprovider:id()) -> [pid()].
parallel_apply(DocsList, Ref, ProviderId) ->
    Master = self(),
    lists:map(fun(DocList) ->
        spawn(fun() ->
            SlaveAns = apply_docs_list(lists:reverse(DocList), ProviderId, #dbsync_application_result{}),
            Master ! {changes_worker_ans, Ref, self(), SlaveAns}
        end)
    end, DocsList).

%% @private
-spec apply_docs_list([[datastore:doc()]], oneprovider:id(), dbsync_successful_application_result()) ->
    dbsync_successful_application_result().
apply_docs_list([], _ProviderId, Acc) ->
    Acc;
apply_docs_list([Doc | DocList], ProviderId,
    #dbsync_application_result{successful = Applied, erroneous = AppliedWithError} = Acc) ->
    case dbsync_changes:apply(Doc, ProviderId) of
        {ok, undefined} ->
            apply_docs_list(DocList, ProviderId, Acc);
        {ok, #remote_mutation_info{} = UpdateDesc} ->
            Acc2 = Acc#dbsync_application_result{successful = [UpdateDesc | Applied]},
            apply_docs_list(DocList, ProviderId, Acc2);
        {?REMOTE_DOC_ALREADY_EXISTS, #remote_mutation_info{} = UpdateDesc} ->
            Acc2 = Acc#dbsync_application_result{erroneous = [UpdateDesc | AppliedWithError]},
            apply_docs_list(DocList, ProviderId, Acc2);
        {error, Seq} ->
            Acc#dbsync_application_result{min_erroneous_seq = Seq}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gather answers from workers.
%% @end
%%--------------------------------------------------------------------
-spec gather_answers([pid()], reference()) -> dbsync_application_result().
gather_answers(SlavesList, Ref) ->
    gather_answers(SlavesList, Ref, #dbsync_application_result{}, false).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gather appropriate number of workers' answers.
%% @end
%%--------------------------------------------------------------------
-spec gather_answers([pid()], reference(), dbsync_successful_application_result(), boolean()) ->
    dbsync_application_result().
gather_answers([], _Ref, Ans, _FinalCheck) ->
    Ans;
gather_answers(Pids, Ref, TmpAns, FinalCheck) ->
    receive
        {changes_worker_ans, Ref, Pid, Ans} ->
            MergedAns = merge_result_records(Ans, TmpAns),
            gather_answers(Pids -- [Pid], Ref, MergedAns, FinalCheck)
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

-spec merge_result_records(dbsync_successful_application_result(), dbsync_successful_application_result()) ->
    dbsync_successful_application_result().
merge_result_records(#dbsync_application_result{
    successful = Applied1,
    erroneous = AppliedWithError1,
    min_erroneous_seq = SmallestSeqWithError1
}, #dbsync_application_result{
    successful = Applied2,
    erroneous = AppliedWithError2,
    min_erroneous_seq = SmallestSeqWithError2
}) ->
    SmallestSeqWithError = case {SmallestSeqWithError1, SmallestSeqWithError2} of
        {undefined, _} -> SmallestSeqWithError2;
        {_, undefined} -> SmallestSeqWithError1;
        _ -> min(SmallestSeqWithError1, SmallestSeqWithError2)
    end,

    #dbsync_application_result{
        successful = Applied1 ++ Applied2,
        erroneous = AppliedWithError1 ++ AppliedWithError2,
        min_erroneous_seq = SmallestSeqWithError
    }.

%%--------------------------------------------------------------------
%% @doc
%% Returns model context used during application of changes.
%% Warning: if any traverse callback module uses other sync info than one provided by tree_traverse, this function
%% has to be extended to parse #document and get callback module.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx(model()) -> ctx().
get_ctx(Model) ->
    case Model of
        traverse_task ->
            Ctx = tree_traverse:get_sync_info(),
            datastore_model_default:set_defaults(Ctx#{model => Model});
        _ ->
            datastore_model_default:get_ctx(Model)
    end.