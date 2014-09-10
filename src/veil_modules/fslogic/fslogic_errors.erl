%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013, ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module provides error translators for generic fslogic errors
%% @end
%% ===================================================================
-module(fslogic_errors).
-author("Rafal Slota").

-include("fuse_messages_pb.hrl").
-include("communication_protocol_pb.hrl").
-include_lib("ctool/include/logging.hrl").
-include("veil_modules/fslogic/fslogic.hrl").

%% API
-export([gen_error_message/2, normalize_error_code/1, gen_error_code/1, posix_to_veilerror/1, veilerror_to_posix/1]).

%% ====================================================================
%% API functions
%% ====================================================================


%% gen_error_code/1
%% ====================================================================
%% @doc Translates given error that was thrown to {ErrorCode :: fslogic_error(), ErrorDetails :: term()}.
%%      This function is intended to be extended when new translation is needed.
-spec gen_error_code(Error :: term()) -> {ErrorCode :: fslogic_error(), ErrorDetails :: term()}.
%% ====================================================================
gen_error_code({error, Reason}) ->
    gen_error_code(Reason);
gen_error_code(file_not_found) ->
    {?VENOENT, no_details};
gen_error_code({permission_denied, Details}) ->
    {?VEACCES, {permission_denied, Details}};
gen_error_code(user_not_found) ->
    {?VEPERM, user_not_found};
gen_error_code(user_doc_not_found) ->
    {?VEPERM, user_doc_not_found};
gen_error_code(invalid_group_access) ->
    {?VEACCES, invalid_group_access};
gen_error_code(file_exists) ->
    {?VEEXIST, file_already_exists};

%% Generic translations below. All custom translations shall be defined ^above this line.
gen_error_code(ErrorCode) when is_list(ErrorCode) ->
    case lists:member(ErrorCode, ?ALL_ERROR_CODES) of
        true    -> {ErrorCode, no_details};
        false   -> {?VEREMOTEIO, ErrorCode}
    end;
gen_error_code({ErrorCode, ErrorDetails}) when is_list(ErrorCode) ->
    case lists:member(ErrorCode, ?ALL_ERROR_CODES) of
        true    -> {ErrorCode, ErrorDetails};
        false   -> {?VEREMOTEIO, {ErrorCode, ErrorDetails}}
    end;
gen_error_code(UnknownReason) ->
    {?VEREMOTEIO, UnknownReason}.

%% gen_error_message/2
%% ====================================================================
%% @doc Convinience method that returns protobuf answer message that is build base on given error code
%%      and type of request.
%% @end
-spec gen_error_message(RecordName :: atom(), VeilError :: string()) -> tuple() | no_return().
%% ====================================================================
gen_error_message(getfileattr, Error) ->
    #fileattr{answer = Error, mode = 0, uid = -1, gid = -1, atime = 0, ctime = 0, mtime = 0, type = ""};
gen_error_message(setfileusermetadata, Error) ->
    #atom{value = Error};
gen_error_message(getfileuuid, Error) ->
    #fileuuid{answer = Error, uuid = ""};
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
gen_error_message(createfileack, Error) ->
    #atom{value = Error};
gen_error_message(createstoragetestfilerequest, _) ->
    #createstoragetestfileresponse{answer = false};
gen_error_message(storagetestfilemodifiedrequest, _) ->
    #storagetestfilemodifiedresponse{answer = false};
gen_error_message(clientstorageinfo, Error) ->
    #atom{value = Error};
gen_error_message(RecordName, _Error) ->
    ?error("Unsupported record: ~p", [RecordName]),
    throw({unsupported_record, RecordName}).


%% normalize_error_code/1
%% ====================================================================
%% @doc Normalizes format of given ErrorCode. It's unspecified if fslogic_error() macros are
%%      string() or atom(). This method shall behave accordingly to current fslogic_error() type implementation.
-spec normalize_error_code(ErrorCode :: atom() | string()) -> ErrorCode :: fslogic_error().
%% ====================================================================
normalize_error_code(ErrorCode) when is_atom(ErrorCode) ->
    atom_to_list(ErrorCode);
normalize_error_code(ErrorCode) when is_list(ErrorCode) ->
    ErrorCode.


%% posix_to_veilerror/1
%% ====================================================================
%% @doc Translates POSIX error code to internal fslogic_error().
-spec posix_to_veilerror(POSIXErrorCode :: integer()) -> ErrorCode :: fslogic_error().
%% ====================================================================
posix_to_veilerror(POSIX) when POSIX < 0 -> %% All error codes are currently negative, so translate accordingly
    posix_to_veilerror(-POSIX);
posix_to_veilerror(1) ->
    ?VEPERM;
posix_to_veilerror(2) ->
    ?VENOENT;
posix_to_veilerror(17) ->
    ?VEEXIST;
posix_to_veilerror(13) ->
    ?VEACCES;
posix_to_veilerror(122) ->
    ?VEDQUOT;
posix_to_veilerror(22) ->
    ?VEINVAL;
posix_to_veilerror(39) ->
    ?VENOTEMPTY;
posix_to_veilerror(95) ->
    ?VENOTSUP;
posix_to_veilerror(_Unkwn) ->
    ?VEREMOTEIO.

%% veilerror_to_posix/1
%% ====================================================================
%% @doc Translates internal fslogic_error() to POSIX error code.
-spec veilerror_to_posix(ErrorCode :: fslogic_error()) -> POSIXErrorCode :: non_neg_integer().
%% ====================================================================
veilerror_to_posix(?VOK) ->
    0;
veilerror_to_posix(?VEPERM) ->
    1;
veilerror_to_posix(?VENOENT) ->
    2;
veilerror_to_posix(?VEEXIST) ->
    17;
veilerror_to_posix(?VEACCES) ->
    13;
veilerror_to_posix(?VEDQUOT) ->
    122;
veilerror_to_posix(?VEINVAL) ->
    22;
veilerror_to_posix(?VENOTEMPTY) ->
    39;
veilerror_to_posix(?VENOTSUP) ->
    95;
veilerror_to_posix(_Unkwn) ->
    121.


%% ====================================================================
%% Internal functions
%% ====================================================================
