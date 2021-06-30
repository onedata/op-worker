%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This is a helper for calculating file's checksums using lfm.
%%% @end
%%%-------------------------------------------------------------------
-module(file_checksum).
-author("Jakub Kudzia").

-include("modules/logical_file_manager/lfm.hrl").
-include("modules/logical_file_manager/utils/file_checksum.hrl").

%% API
-export([calculate/3, get/2]).

-type algorithm() :: ?MD5 | ?SHA1 | ?SHA256 | ?SHA512.
-type algorithms() :: [algorithm()].

-type buffer() :: term(). % crypto:hash_state() is not an exported type
-type buffers() :: #{algorithm() => buffer()}.

-type checksum() :: binary().
-opaque checksums() :: #{algorithm() => checksum()}.

-export_type([algorithm/0, algorithms/0, checksum/0, checksums/0]).

-define(BUFFER_SIZE, 52428800). % 50 M

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% This function calculates checksums of given file.
%% Checksums are calculated for all Algorithms.
%% It returns data structure from which values for specific
%% algorithms should be extracted using ?MODULE:get/2 function.
%% @end
%%--------------------------------------------------------------------
-spec calculate(file_ctx:ctx(), user_ctx:ctx(), algorithm() | algorithms()) -> checksums().
calculate(FileCtx, UserCtx, Algorithms) ->
    Buffers = init_buffers(utils:ensure_list(Algorithms)),
    SessionId = user_ctx:get_session_id(UserCtx),
    Guid = file_ctx:get_logical_guid_const(FileCtx),
    {ok, Handle} = lfm:open(SessionId, ?FILE_REF(Guid), read),
    Checksums = calculate_file_checksums_helper(Handle, 0, Buffers),
    lfm:release(Handle),
    Checksums.


-spec get(algorithm(), checksums()) -> checksum().
get(Algorithm, Checksums) ->
    maps:get(Algorithm, Checksums).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec init_buffers(algorithms()) -> buffers().
init_buffers(Algorithms) ->
    lists:foldl(fun(Algorithm, Acc) ->
        Acc#{Algorithm => crypto:hash_init(ensure_compatible_algorithm_name(Algorithm))}
    end, #{}, Algorithms).


%% @private
-spec update_buffers(buffers(), binary()) -> buffers().
update_buffers(Buffers, Data) ->
    maps:map(fun(_, HashState) ->
        crypto:hash_update(HashState, Data)
    end, Buffers).


%% @private
-spec calculate_hashes(buffers()) -> checksums().
calculate_hashes(Buffers) ->
    maps:map(fun(_, HashState) ->
        hex_utils:hex(crypto:hash_final(HashState))
    end, Buffers).


%% @private
-spec calculate_file_checksums_helper(lfm:handle(), non_neg_integer(), buffers()) ->
    checksums().
calculate_file_checksums_helper(Handle, Offset, Buffers) ->
    {ok, NewHandle, Content} = lfm:check_size_and_read(Handle, Offset, ?BUFFER_SIZE),
    UpdatedBuffers = update_buffers(Buffers, Content),
    ContentSize = byte_size(Content),
    case ContentSize =:= 0 of
        true -> calculate_hashes(UpdatedBuffers);
        false -> calculate_file_checksums_helper(NewHandle, Offset + ContentSize, UpdatedBuffers)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Maps checksum algorithm name to atom compatible with crypto module.
%% @end
%%--------------------------------------------------------------------
-spec ensure_compatible_algorithm_name(algorithm()) -> atom().
ensure_compatible_algorithm_name(?MD5) -> md5;
ensure_compatible_algorithm_name(?SHA1) -> sha;
ensure_compatible_algorithm_name(?SHA256) -> sha256;
ensure_compatible_algorithm_name(?SHA512) -> sha512.