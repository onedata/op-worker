%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Strategy for handling enoent.
%%% @end
%%%-------------------------------------------------------------------
-module(enoent_handling).
-author("Rafal Slota").
-behavior(space_strategy_behaviour).

-include("modules/storage_sync/strategy_config.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%%%===================================================================
%%% Types
%%%===================================================================

%%%===================================================================
%%% Exports
%%%===================================================================

%% Types
-export_type([]).

%% space_strategy_behaviour callbacks
-export([available_strategies/0, strategy_init_jobs/3, strategy_handle_job/1,
    main_worker_pool/0, strategy_merge_result/2, strategy_merge_result/3,
    worker_pools_config/0
]).

%% API
-export([get_canonical_file_entry/2]).

%%%===================================================================
%%% space_strategy_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link space_strategy_behaviour} callback available_strategies/0.
%% @end
%%--------------------------------------------------------------------
-spec available_strategies() -> [space_strategy:definition()].
available_strategies() ->
    [
        #space_strategy{result_merge_type = merge_all, name = error_passthrough, arguments = [],
            description = <<"Ignore all file conflicts">>},
        #space_strategy{result_merge_type = merge_all, name = check_locally, arguments = [],
            description = <<"TODO">>},
        #space_strategy{result_merge_type = return_first, name = check_globally, arguments = [],
            description = <<"TODO">>}
    ].

%%--------------------------------------------------------------------
%% @doc
%% {@link space_strategy_behaviour} callback strategy_init_jobs/3.
%% @end
%%--------------------------------------------------------------------
-spec strategy_init_jobs(space_strategy:name(), space_strategy:arguments(), space_strategy:job_data()) ->
    [space_strategy:job()].
strategy_init_jobs(check_globally, StrategyArgs, InitData) ->
    [#space_strategy_job{strategy_name = check_globally, strategy_args = StrategyArgs,
        data = InitData#{provider_id => oneprovider:get_provider_id()}}];
strategy_init_jobs(StrategyName, StrategyArgs, InitData) ->
    [#space_strategy_job{strategy_name = StrategyName, strategy_args = StrategyArgs, data = InitData}].

%%--------------------------------------------------------------------
%% @doc
%% {@link space_strategy_behaviour} callback strategy_handle_job/1.
%% @end
%%--------------------------------------------------------------------
-spec strategy_handle_job(space_strategy:job()) -> {space_strategy:job_result(), [space_strategy:job()]}.
strategy_handle_job(#space_strategy_job{strategy_name = error_passthrough, data = #{response := Response}}) ->
    {Response, []};
strategy_handle_job(#space_strategy_job{strategy_name = check_globally, data = Data} = Job) ->
    #{
        response := OriginalResponse,
        provider_id := ProviderId,
        space_id := SpaceId,
        ctx := CTX,
        request := Request
    } = Data,
    {ok, #document{value = #od_space{providers = ProviderIds0}}} = od_space:get(SpaceId),
    case oneprovider:get_provider_id() == ProviderId of
        true ->
            {MergeType, Jobs} = space_sync_worker:init(?MODULE, SpaceId, undefined, Data),
            case space_sync_worker:run({MergeType, [Job#space_strategy_job{strategy_name = check_locally} || Job <- Jobs]}) of
                #fuse_response{status = #status{code = ?OK}} = Response ->
                    {Response, []};
                OtherResp ->
                    ProviderIds = ProviderIds0 -- [oneprovider:get_provider_id()],
                    SessionId = user_ctx:get_session_id(CTX),
                    {ok, #document{value = #session{proxy_via = ProxyVia}}} = session:get(SessionId),
                    NewJobs = case lists:member(ProxyVia, ProviderIds) of
                        true -> [];
                        false -> [Job#space_strategy_job{data = Data#{provider_id => RProviderId}} || RProviderId <- ProviderIds]
                    end,
                    {OriginalResponse, NewJobs}
            end;
        false ->
            {fslogic_remote:reroute(CTX, ProviderId, Request), []}
    end;
strategy_handle_job(#space_strategy_job{strategy_name = check_locally, data = Data}) ->
    #{
        space_id := SpaceId,
        response := Response,
        path := Path,
        ctx := CTX
    } = Data,
    {ok, #document{value = #space_storage{storage_ids = StorageIds}}} = space_storage:get(SpaceId),

    MaybeAttrs = lists:map(
        fun(StorageId) ->
            {ok, Tokens} = fslogic_path:split_skipping_dots(Path),
            {path, LogicalPath} = get_canonical_file_entry(CTX, Tokens),
            FileId = filename_mapping:to_storage_path(SpaceId, StorageId, LogicalPath),
            InitialImportJobData =
                #{
                    last_import_time => 0,
                    space_id => SpaceId,
                    storage_id => StorageId,
                    storage_logical_file_id => FileId,
                    max_depth => 1
                },

            Init = space_sync_worker:init(storage_update, SpaceId, StorageId, InitialImportJobData),
            case lists:member(ok, space_sync_worker:run(Init)) of
                true ->
                    {ok, #document{key = Uuid}} = file_meta:get({path, LogicalPath}),
                    Guid = fslogic_uuid:uuid_to_guid(Uuid),
                    File = file_ctx:new_by_guid(Guid),
                    (catch attr_req:get_file_attr_insecure(CTX, File));
                _ ->
                    undefined
            end
        end, StorageIds),

    case [Attr || #fuse_response{status = #status{code = ?OK}} = Attr <- MaybeAttrs] of
        [Resp | _] ->
            {Resp, []};
        [] ->
            {Response, []}
    end.

%%--------------------------------------------------------------------
%% @doc
%% {@link space_strategy_behaviour} callback strategy_merge_result/2.
%% @end
%%--------------------------------------------------------------------
-spec strategy_merge_result(ChildrenJobs :: [space_strategy:job()],
    ChildrenResults :: [space_strategy:job_result()]) ->
    space_strategy:job_result().
strategy_merge_result([_Job | _], [#fuse_response{status = #status{code = ?OK}} = Result | _]) ->
    Result;
strategy_merge_result(_Jobs, [OnlyResponse]) ->
    OnlyResponse;
strategy_merge_result([_ | JobsR], [_ | R]) ->
    strategy_merge_result(JobsR, R).

%%--------------------------------------------------------------------
%% @doc
%% {@link space_strategy_behaviour} callback strategy_merge_result/3.
%% @end
%%--------------------------------------------------------------------
-spec strategy_merge_result(space_strategy:job(), LocalResult :: space_strategy:job_result(),
    ChildrenResult :: space_strategy:job_result()) ->
    space_strategy:job_result().
strategy_merge_result(#space_strategy_job{}, _LocalResult, #fuse_response{status = #status{code = ?OK}, fuse_response = FResponse} = ChildrenResult) ->
    #file_attr{guid = FileGuid, provider_id = ProviderId} = FResponse,
    case oneprovider:get_provider_id() of
        ProviderId -> ChildrenResult;
        _ ->
            #file_attr{guid = FileGuid} = FResponse,
            file_force_proxy:save(#document{key = FileGuid, value = #file_force_proxy{provider_id = ProviderId}}),
            ChildrenResult
    end;
strategy_merge_result(#space_strategy_job{}, LocalResult, _ChildrenResult) ->
    LocalResult.

%%--------------------------------------------------------------------
%% @doc
%% {@link space_strategy_behaviour} callback worker_pools_config/0.
%% @end
%%--------------------------------------------------------------------
-spec worker_pools_config() -> [{worker_pool:name(), non_neg_integer()}].
worker_pools_config() ->
    space_strategy:default_worker_pool_config().

%%--------------------------------------------------------------------
%% @doc
%% {@link space_strategy_behaviour} callback main_worker_pool/0.
%% @end
%%--------------------------------------------------------------------
-spec main_worker_pool() -> worker_pool:name().
main_worker_pool() ->
    space_strategy:default_main_worker_pool().



%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Gets file's full name.
%% @end
%%--------------------------------------------------------------------
-spec get_canonical_file_entry(user_ctx:ctx(), [file_meta:path()]) ->
    file_meta:entry() | no_return().
get_canonical_file_entry(UserCtx, Tokens) ->
    case session:is_special(user_ctx:get_session_id(UserCtx)) of
        true ->
            {path, fslogic_path:join(Tokens)};
        false ->
            get_canonical_file_entry_for_user(UserCtx, Tokens)
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Gets file's full name, checking user defined space names.
%% @end
%%--------------------------------------------------------------------
-spec get_canonical_file_entry_for_user(user_ctx:ctx(), [file_meta:path()]) -> file_meta:entry() | no_return().
get_canonical_file_entry_for_user(UserCtx, [<<?DIRECTORY_SEPARATOR>>]) ->
    UserId = user_ctx:get_user_id(UserCtx),
    {uuid, fslogic_uuid:user_root_dir_uuid(UserId)};
get_canonical_file_entry_for_user(UserCtx, [<<?DIRECTORY_SEPARATOR>>, SpaceName | Tokens]) ->
    #document{value = #od_user{space_aliases = Spaces}} = user_ctx:get_user(UserCtx),
    case lists:keyfind(SpaceName, 2, Spaces) of
        false ->
            throw(?ENOENT);
        {SpaceId, _} ->
            {path, fslogic_path:join(
                [<<?DIRECTORY_SEPARATOR>>, SpaceId | Tokens])}
    end;
get_canonical_file_entry_for_user(_UserCtx, Tokens) ->
    Path = fslogic_path:join([<<?DIRECTORY_SEPARATOR>> | Tokens]),
    {path, Path}.
