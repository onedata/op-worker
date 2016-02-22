%%%--------------------------------------------------------------------
%%% @author Mateusz Paciorek
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module provides functions operating on file timestamps
%%% @end
%%%--------------------------------------------------------------------
-module(fslogic_times).
-author("Mateusz Paciorek").

-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").

%% API
-export([calculate_atime/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Returns value of atime to be set for given file
%% or atom "actual" if time should not be changed
%% @end
%%--------------------------------------------------------------------
-spec calculate_atime(FileEntry :: fslogic_worker:file()) -> integer() | atom().
calculate_atime(FileEntry) ->
    {ok, #document{value = #file_meta{
        atime = ATime,
        mtime = MTime,
        ctime = CTime}}
    } = file_meta:get(FileEntry),
    CurrentTime = erlang:system_time(seconds),
    case ATime of
        Outdated when Outdated =< MTime orelse Outdated =< CTime ->
            CurrentTime;
        _ ->
            case (CurrentTime - ATime) of
                TooLongTime when TooLongTime > (24 * 60 * 60) ->
                    CurrentTime;
                _ ->
                    actual
            end
    end.
