%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013, ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: Write me !
%% @end
%% ===================================================================
-module(fslogic_errors).
-author("Rafal Slota").

-include("fuse_messages_pb.hrl").
-include("communication_protocol_pb.hrl").
-include("logging.hrl").

%% API
-export([gen_error_message/2, normalize_error_code/1]).

%% ====================================================================
%% API functions
%% ====================================================================

%% gen_error_message/2
%% ====================================================================
%% @doc Convinience method that returns protobuf answer message that is build base on given error code
%%      and type of request.
%% @end
-spec gen_error_message(RecordName :: atom(), VeilError :: string()) -> tuple() | no_return().
%% ====================================================================
gen_error_message(getfileattr, Error) ->
    #fileattr{answer = Error, mode = 0, uid = -1, gid = -1, atime = 0, ctime = 0, mtime = 0, type = ""};
gen_error_message(getfilelocation, Error) ->
    #filelocation{answer = Error, storage_id = -1, file_id = "", validity = 0};
gen_error_message(getnewfilelocation, Error) ->
    #filelocation{answer = Error, storage_id = -1, file_id = "", validity = 0};
gen_error_message(filenotused, Error) ->
    #atom{value = Error};
gen_error_message(renamefile, Error) ->
    #atom{value = Error};
gen_error_message(deletefile, Error) ->
    #atom{value = Error};
gen_error_message(createdir, Error) ->
    #atom{value = Error};
gen_error_message(changefileowner, Error) ->
    #atom{value = Error};
gen_error_message(changefilegroup, Error) ->
    #atom{value = Error};
gen_error_message(changefileperms, Error) ->
    #atom{value = Error};
gen_error_message(updatetimes, Error) ->
    #atom{value = Error};
gen_error_message(createlink, Error) ->
    #atom{value = Error};
gen_error_message(renewfilelocation, Error) ->
    #filelocationvalidity{answer = Error, validity = 0};
gen_error_message(getfilechildren, Error) ->
    #filechildren{answer = Error, child_logic_name = []};
gen_error_message(getlink, Error) ->
    #linkinfo{answer = Error, file_logic_name = ""};
gen_error_message(testchannel, Error) ->
    #atom{value = Error};
gen_error_message(RecordName, _Error) ->
    ?error("Unsupported record: ~p", [RecordName]),
    throw({unsupported_record, RecordName}).

normalize_error_code(ErrorCode) when is_atom(ErrorCode) ->
    atom_to_list(ErrorCode);
normalize_error_code(ErrorCode) when is_list(ErrorCode) ->
    ErrorCode.

%% ====================================================================
%% Internal functions
%% ====================================================================
