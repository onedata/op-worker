%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for management of file attr caches.
%%% @end
%%%-------------------------------------------------------------------
-module(file_protection_flags_cache).
-author("Bartosz Walkowicz").

-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/data_access_control.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([init/1, get_effective_flags/1, invalidate/1]).

-define(FILE_PROTECTION_FLAGS_CACHE_NAME(SpaceId),
    binary_to_atom(<<?MODULE_STRING, "_", SpaceId/binary>>, utf8)
).
-define(DEFAULT_CHECK_FREQUENCY, 30000).  % 30 s
-define(DEFAULT_CACHE_SIZE, 20000).

-define(UNABLE_TO_INITIALIZE_LOG_FMT,
    "Unable to initialize file attr bounded cache for ~p space(s) due to: ~p"
).


%%%===================================================================
%%% API
%%%===================================================================


-spec init(od_space:id() | all) -> boolean().
init(all) ->
    try provider_logic:get_spaces() of
        {ok, SpaceIds} ->
            lists:all(fun init/1, SpaceIds);
        ?ERROR_NO_CONNECTION_TO_ONEZONE ->
            ?debug(?UNABLE_TO_INITIALIZE_LOG_FMT, [all, ?ERROR_NO_CONNECTION_TO_ONEZONE]),
            false;
        ?ERROR_UNREGISTERED_ONEPROVIDER ->
            ?debug(?UNABLE_TO_INITIALIZE_LOG_FMT, [all, ?ERROR_UNREGISTERED_ONEPROVIDER]),
            false;
        {error, _} = Error ->
            ?critical(?UNABLE_TO_INITIALIZE_LOG_FMT, [all, Error]),
            false
    catch Type:Reason ->
        ?critical_stacktrace(?UNABLE_TO_INITIALIZE_LOG_FMT, [all, {Type, Reason}]),
        false
    end;
init(SpaceId) ->
    CacheName = ?FILE_PROTECTION_FLAGS_CACHE_NAME(SpaceId),

    try
        case bounded_cache:cache_exists(CacheName) of
            true ->
                true;
            _ ->
                Opts = #{
                    worker => true,
                    check_frequency => ?DEFAULT_CHECK_FREQUENCY,
                    size => ?DEFAULT_CACHE_SIZE
                },
                case bounded_cache:init_cache(CacheName, Opts) of
                    ok ->
                        true;
                    {error, _} = Error ->
                        ?critical(?UNABLE_TO_INITIALIZE_LOG_FMT, [SpaceId, Error]),
                        false
                end
        end
    catch Type:Reason ->
        ?critical_stacktrace(?UNABLE_TO_INITIALIZE_LOG_FMT, [SpaceId, {Type, Reason}]),
        false
    end.


-spec get_effective_flags(file_ctx:ctx()) ->
    {data_access_control:bitmask(), file_ctx:ctx()} | no_return().
get_effective_flags(FileCtx) ->
    case file_ctx:is_root_dir_const(FileCtx) of
        true ->
            {?no_flags_mask, FileCtx};
        false ->
            SpaceId = file_ctx:get_space_id_const(FileCtx),
            CacheName = ?FILE_PROTECTION_FLAGS_CACHE_NAME(SpaceId),
            {FileDoc, FileCtx2} = file_ctx:get_file_doc_including_deleted(FileCtx),

            Callback = fun([#document{
                key = Uuid,
                value = #file_meta{protection_flags = Flags}
            }, ParentValue, CalculationInfo]) ->
                case fslogic_uuid:is_root_dir_uuid(Uuid) orelse fslogic_uuid:is_space_dir_uuid(Uuid) of
                    true -> {ok, Flags, CalculationInfo};
                    false -> {ok, ?set_flags(ParentValue, Flags), CalculationInfo}
                end
            end,

            case effective_value:get_or_calculate(CacheName, FileDoc, Callback) of
                {ok, EffFlags, _} ->
                    {EffFlags, FileCtx2};
                {error, {file_meta_missing, _}} ->
                    throw(?ERROR_NOT_FOUND)
            end
    end.


-spec invalidate(od_space:id()) -> ok.
invalidate(SpaceId) ->
    ok = invalidate_file_protection_flags_cache(SpaceId),
    ok = permissions_cache:invalidate_on_node().


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec invalidate_file_protection_flags_cache(od_space:id()) -> ok.
invalidate_file_protection_flags_cache(SpaceId) ->
    effective_value:invalidate(?FILE_PROTECTION_FLAGS_CACHE_NAME(SpaceId)).
