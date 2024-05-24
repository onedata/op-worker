%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module implementing file meta posthook (see file_meta_posthooks.erl)
%%% to handle situation when hardlink file_meta document is synchronized
%%% before file_meta of file it references.
%%% @end
%%%-------------------------------------------------------------------
-module(file_meta_hardlinks_posthooks).
-author("Michal Stanisz").

-behaviour(file_meta_posthooks_behaviour).

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    add_posthook/2,
    execute_posthook/2,
    encode_file_meta_posthook_args/2,
    decode_file_meta_posthook_args/2
]).


%%%===================================================================
%%% API
%%%===================================================================

-spec add_posthook(od_space:id(), file_meta:uuid()) -> ok.
add_posthook(SpaceId, HardlinkUuid) ->
    FileUuid = fslogic_file_id:ensure_referenced_uuid(HardlinkUuid),
    file_meta_posthooks:add_hook({file_meta_missing, FileUuid}, <<"hardlink missing base doc">>,
        SpaceId, ?MODULE, execute_posthook, [SpaceId, HardlinkUuid]).


-spec execute_posthook(od_space:id(), file_meta:uuid()) -> ok.
execute_posthook(SpaceId, HardlinkUuid) ->
    {ok, HardlinkDoc} = file_meta:get_including_deleted(HardlinkUuid),
    dbsync_events:change_replicated(SpaceId, HardlinkDoc).


-spec encode_file_meta_posthook_args(file_meta_posthooks:function_name(), [term()]) ->
    file_meta_posthooks:encoded_args().
encode_file_meta_posthook_args(execute_posthook, [_SpaceId, _HardlinkUuid] = Args) ->
    term_to_binary(Args).


-spec decode_file_meta_posthook_args(file_meta_posthooks:function_name(), file_meta_posthooks:encoded_args()) ->
    [term()].
decode_file_meta_posthook_args(_, EncodedArgs) ->
    binary_to_term(EncodedArgs).

