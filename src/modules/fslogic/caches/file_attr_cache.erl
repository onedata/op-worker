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
-module(file_attr_cache).
-author("Bartosz Walkowicz").

-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([init_group/0, init_caches/1, invalidate_caches/1]).
-export([name/1]).

-define(FILE_ATTR_CACHE_GROUP, <<"file_attr_group">>).
-define(FILE_ATTR_CACHE_NAME(SpaceId),
    binary_to_atom(<<?MODULE_STRING, "_", SpaceId/binary>>, utf8)
).

-define(DEFAULT_CHECK_FREQUENCY, 30000).  % 30 s
-define(DEFAULT_CACHE_SIZE, 20000).

-define(UNABLE_TO_INITIALIZE_LOG_MSG,
    "Unable to initialize file attr bounded cache for ~p space(s) due to: ~p"
).


%%%===================================================================
%%% API
%%%===================================================================


-spec name(od_space:id()) -> atom().
name(SpaceId) ->
    ?FILE_ATTR_CACHE_NAME(SpaceId).


-spec init_group() -> ok | {error, term()}.
init_group() ->
    bounded_cache:init_group(?FILE_ATTR_CACHE_GROUP, #{
        check_frequency => ?DEFAULT_CHECK_FREQUENCY,
        size => ?DEFAULT_CACHE_SIZE,
        worker => true
    }).


-spec init_caches(od_space:id() | all) -> boolean().
init_caches(all) ->
    try provider_logic:get_spaces() of
        {ok, SpaceIds} ->
            lists:all(fun init_caches/1, SpaceIds);
        ?ERROR_NO_CONNECTION_TO_ONEZONE ->
            ?debug(?UNABLE_TO_INITIALIZE_LOG_MSG, [all, ?ERROR_NO_CONNECTION_TO_ONEZONE]),
            false;
        ?ERROR_UNREGISTERED_ONEPROVIDER ->
            ?debug(?UNABLE_TO_INITIALIZE_LOG_MSG, [all, ?ERROR_UNREGISTERED_ONEPROVIDER]),
            false;
        {error, _} = Error ->
            ?critical(?UNABLE_TO_INITIALIZE_LOG_MSG, [all, Error]),
            false
    catch Type:Reason ->
        ?critical_stacktrace(?UNABLE_TO_INITIALIZE_LOG_MSG, [all, {Type, Reason}]),
        false
    end;
init_caches(SpaceId) ->
    CacheName = ?FILE_ATTR_CACHE_NAME(SpaceId),

    try
        case bounded_cache:cache_exists(CacheName) of
            true ->
                true;
            _ ->
                Opts = #{group => ?FILE_ATTR_CACHE_GROUP},
                case bounded_cache:init_cache(CacheName, Opts) of
                    ok ->
                        true;
                    {error, _} = Error ->
                        ?critical(?UNABLE_TO_INITIALIZE_LOG_MSG, [SpaceId, Error]),
                        false
                end
        end
    catch Type:Reason ->
        ?critical_stacktrace(?UNABLE_TO_INITIALIZE_LOG_MSG, [SpaceId, {Type, Reason}]),
        false
    end.


-spec invalidate_caches(od_space:id()) -> ok.
invalidate_caches(SpaceId) ->
    ok = effective_value:invalidate(?FILE_ATTR_CACHE_NAME(SpaceId)),
    ok = permissions_cache:invalidate_on_node().
