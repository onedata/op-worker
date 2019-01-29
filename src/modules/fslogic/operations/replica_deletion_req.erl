%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handling requests for deleting file
%%% replicas.
%%% @end
%%%-------------------------------------------------------------------
-module(replica_deletion_req).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([delete_blocks/3]).


%%%===================================================================
%%% API
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% Deletes given Blocks in file associated with given FileCtx.
%% Before deletion, checks whether current Version is equal or
%% lesser than AllowedVV
%% @end
%%-------------------------------------------------------------------
-spec delete_blocks(file_ctx:ctx(), [sync_req:block()],
    version_vector:version_vector()) -> ok | {error, term()}.
delete_blocks(FileCtx, _Blocks, AllowedVV) ->
    %todo VFS-3728 implement deletion of file parts (blocks)
    replica_synchronizer:delete_whole_file_replica(FileCtx, AllowedVV).
