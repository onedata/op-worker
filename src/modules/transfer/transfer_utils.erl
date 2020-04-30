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

-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/privileges.hrl").

%% API
-export([encode_pid/1, decode_pid/1]).


%%%===================================================================
%%% API
%%%===================================================================


-spec encode_pid(pid()) -> binary().
encode_pid(Pid) ->
    % todo remove after VFS-3657
    list_to_binary(pid_to_list(Pid)).


-spec decode_pid(binary()) -> pid().
decode_pid(Pid) ->
    % todo remove after VFS-3657
    list_to_pid(binary_to_list(Pid)).
