%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%% @doc This module performs attributes-related operations of lfm_submodules.
%%% @end
%%%-------------------------------------------------------------------
-module(lfm_attrs).

-include("types.hrl").
-include("errors.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").

%% API
-export([stat/2, set_xattr/3, get_xattr/2, remove_xattr/2, list_xattr/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns file attributes.
%%
%% @end
%%--------------------------------------------------------------------
-spec stat(fslogic_worker:ctx(), FileKey :: file_id_or_path()) -> {ok, file_attributes()} | error_reply().
stat(#fslogic_ctx{session_id = SessId}, FileEntry) ->
    lfm_utils:call_fslogic(SessId, #get_file_attr{entry = FileEntry},
        fun(#file_attr{} = Attrs) ->
            {ok, Attrs}
        end).


%%--------------------------------------------------------------------
%% @doc
%% Returns file's extended attribute by key.
%%
%% @end
%%--------------------------------------------------------------------
-spec get_xattr(FileKey :: file_key(), Key :: xattr_key()) -> {ok, xattr_value()} | error_reply().
get_xattr(_Path, _Key) ->
    {ok, <<"">>}.


%%--------------------------------------------------------------------
%% @doc
%% Updates file's extended attribute by key.
%%
%% @end
%%--------------------------------------------------------------------
-spec set_xattr(FileKey :: file_key(), Key :: xattr_key(), Value :: xattr_value()) -> ok |  error_reply().
set_xattr(_Path, _Key, _Value) ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Removes file's extended attribute by key.
%%
%% @end
%%--------------------------------------------------------------------
-spec remove_xattr(FileKey :: file_key(), Key :: xattr_key()) -> ok |  error_reply().
remove_xattr(_Path, _Key) ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Returns complete list of extended attributes of a file.
%%
%% @end
%%--------------------------------------------------------------------
-spec list_xattr(FileKey :: file_key()) -> {ok, [{Key :: xattr_key(), Value :: xattr_value()}]} | error_reply().
list_xattr(_Path) ->
    {ok, []}.


