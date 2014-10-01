%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2014, ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module provides error translators for generic remote_files_manager errors
%% @end
%% ===================================================================
-module(remote_files_manager_errors).
-author("Rafal Slota").

-include("remote_file_management_pb.hrl").
-include("communication_protocol_pb.hrl").
-include_lib("ctool/include/logging.hrl").
-include("oneprovider_modules/fslogic/fslogic.hrl").

%% API
-export([gen_error_message/2]).

%% ====================================================================
%% API functions
%% ====================================================================


%% gen_error_message/2
%% ====================================================================
%% @doc Convinience method that returns protobuf answer message that is build base on given error code
%%      and type of request.
%% @end
-spec gen_error_message(RecordName :: atom(), Error :: string()) -> tuple() | no_return().
%% ====================================================================
gen_error_message(readfile, Error) ->
    #filedata{answer_status = Error};
gen_error_message(writefile, Error) ->
    #writeinfo{answer_status = Error};
gen_error_message(createfile, Error) ->
    #atom{value = Error};
gen_error_message(changepermsatstorage, Error) ->
    #atom{value = Error};
gen_error_message(truncatefile, Error) ->
    #atom{value = Error};
gen_error_message(deletefileatstorage, Error) ->
    #atom{value = Error};
gen_error_message(getattr, Error) ->
    #storageattibutes{answer = Error};
gen_error_message(RecordName, _Error) ->
    ?error("Unsupported record: ~p", [RecordName]),
    throw({unsupported_record, RecordName}).


%% ====================================================================
%% Internal functions
%% ====================================================================
