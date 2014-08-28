%% ===================================================================
%% @author Tomasz Lichon
%% @copyright (C): 2014, ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: FSLogic request handlers for utility operations.
%% @end
%% ===================================================================
-module(fslogic_req_utility).

-include("veil_modules/dao/dao.hrl").
-include_lib("ctool/include/logging.hrl").
-include("fuse_messages_pb.hrl").

%% API
-export([get_file_uuid/1]).

%% get_file_uuid/1
%% ====================================================================
%% @doc Gets file's uuid.
%% @end
-spec get_file_uuid(FullFileName :: string()) -> #fileuuid{} | no_return().
%% ====================================================================
get_file_uuid(FullFileName) ->
    ?debug("get_file_uuid(FullFileName: ~p)", [FullFileName]),
    {ok, FileDoc} = fslogic_objects:get_file(FullFileName),
    #fileuuid{uuid = FileDoc#veil_document.uuid}.