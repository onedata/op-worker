%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This header file defines a session_cookie record, used to store
%% sessions and session memory in database.
%% @end
%% ===================================================================

-ifndef(DAO_COOKIES).
-define(DAO_COOKIES, 1).

%% This record defines a session cookie and is handled as a database document
-record(session_cookie, {
    valid_till = 0 :: integer(),
    session_memory = [] :: [{Key :: term(), Value :: term()}]}).

-endif.