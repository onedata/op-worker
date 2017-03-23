%%%-------------------------------------------------------------------
%%% @author Mateusz Paciorek
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handing request for renaming files or
%%% directories
%%% @end
%%%-------------------------------------------------------------------
-module(rename_req).
-author("Mateusz Paciorek").

-include("global_definitions.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/posix/acl.hrl").

%% API
-export([rename/4]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Renames file or directory
%% @end
%%--------------------------------------------------------------------
-spec rename(user_ctx:ctx(), SourceFileCtx :: file_ctx:ctx(),
    TargetParentFileCtx :: file_ctx:ctx(), TargetName :: file_meta:name()) ->
    no_return() | #fuse_response{}.
rename(_UserCtx, _SourceFileCtx, _TargetParentFileCtx, _TargetName) ->
    #fuse_response{status = #status{code = ?ENOTSUP}}.