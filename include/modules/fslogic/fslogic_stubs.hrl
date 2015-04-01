%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc stubs for protocol
%%% @end
%%%-------------------------------------------------------------------
-author("Rafal Slota").

-ifndef(FSLOGIC_STUBS_HRL).
-define(FSLOGIC_STUBS_HRL, 1).

-record(fusemessage, {input, message_type}).
-record(chmod, {uuid, mode}).
-record(getattr, {uuid}).
-record(space_info, {name, providers}).
-record(atom, {value}).
-record(direntries, {}).
-record(linkinfo, {}).
-record(fileattr, {}).
-record(filelocation, {}).

-endif.