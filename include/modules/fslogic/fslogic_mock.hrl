%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc @todo: Write me!
%%% @end
%%%-------------------------------------------------------------------
-author("Rafal Slota").


-record(fusemessage, {input, message_type}).
-record(chmod, {uuid, mode}).
-record(getattr, {uuid}).
-record(space_info, {name, providers}).
-record(atom, {value}).
-record(direntries, {}).
-record(linkinfo, {}).
-record(fileattr, {}).
-record(filelocation, {}).