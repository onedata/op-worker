%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Utils functions fon operating on transfer record.
%%% @end
%%%-------------------------------------------------------------------
-module(transfer_utils).
-author("Jakub Kudzia").

%% API
-export([encode_pid/1, decode_pid/1]).


%%%===================================================================
%%% API
%%%===================================================================


-spec encode_pid(undefined | pid()) -> binary().
encode_pid(undefined) ->
    undefined;
encode_pid(Pid) when is_pid(Pid)->
    % todo remove after VFS-3657
    list_to_binary(pid_to_list(Pid));
encode_pid(Pid) ->
    Pid.


-spec decode_pid(undefined | binary()) -> pid().
decode_pid(undefined) ->
    undefined;
decode_pid(Pid) ->
    % todo remove after VFS-3657
    list_to_pid(binary_to_list(Pid)).
