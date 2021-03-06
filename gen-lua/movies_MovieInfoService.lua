--
-- Autogenerated by Thrift
--
-- DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
-- @generated
--


local movies_ttype = require 'movies_ttypes'
local Thrift = require 'Thrift'
local TType = Thrift.TType
local TMessageType = Thrift.TMessageType
local __TObject = Thrift.__TObject
local TApplicationException = Thrift.TApplicationException
local __TClient = Thrift.__TClient
local __TProcessor = Thrift.__TProcessor
local ttype = Thrift.ttype
local ttable_size = Thrift.ttable_size
local TException = Thrift.TException


local MovieInfoServiceClient = __TObject.new(__TClient, {
  __type = 'MovieInfoServiceClient'
})

function MovieInfoServiceClient:GetMoviesByIds(movie_ids)
  self:send_GetMoviesByIds(movie_ids)
  return self:recv_GetMoviesByIds(movie_ids)
end

function MovieInfoServiceClient:send_GetMoviesByIds(movie_ids)
  self.oprot:writeMessageBegin('GetMoviesByIds', TMessageType.CALL, self._seqid)
  local args = GetMoviesByIds_args:new{}
  args.movie_ids = movie_ids
  args:write(self.oprot)
  self.oprot:writeMessageEnd()
  self.oprot.trans:flush()
end

function MovieInfoServiceClient:recv_GetMoviesByIds(movie_ids)
  local fname, mtype, rseqid = self.iprot:readMessageBegin()
  if mtype == TMessageType.EXCEPTION then
    local x = TApplicationException:new{}
    x:read(self.iprot)
    self.iprot:readMessageEnd()
    error(x)
  end
  local result = GetMoviesByIds_result:new{}
  result:read(self.iprot)
  self.iprot:readMessageEnd()
  if result.success ~= nil then
    return result.success
  end
  error(TApplicationException:new{errorCode = TApplicationException.MISSING_RESULT})
end

function MovieInfoServiceClient:GetMoviesByTitle(movie_string)
  self:send_GetMoviesByTitle(movie_string)
  return self:recv_GetMoviesByTitle(movie_string)
end

function MovieInfoServiceClient:send_GetMoviesByTitle(movie_string)
  self.oprot:writeMessageBegin('GetMoviesByTitle', TMessageType.CALL, self._seqid)
  local args = GetMoviesByTitle_args:new{}
  args.movie_string = movie_string
  args:write(self.oprot)
  self.oprot:writeMessageEnd()
  self.oprot.trans:flush()
end

function MovieInfoServiceClient:recv_GetMoviesByTitle(movie_string)
  local fname, mtype, rseqid = self.iprot:readMessageBegin()
  if mtype == TMessageType.EXCEPTION then
    local x = TApplicationException:new{}
    x:read(self.iprot)
    self.iprot:readMessageEnd()
    error(x)
  end
  local result = GetMoviesByTitle_result:new{}
  result:read(self.iprot)
  self.iprot:readMessageEnd()
  if result.success ~= nil then
    return result.success
  elseif result.se then
    error(result.se)
  end
  error(TApplicationException:new{errorCode = TApplicationException.MISSING_RESULT})
end

local MovieInfoServiceIface = __TObject:new{
  __type = 'MovieInfoServiceIface'
}


local MovieInfoServiceProcessor = __TObject.new(__TProcessor
, {
 __type = 'MovieInfoServiceProcessor'
})

function MovieInfoServiceProcessor:process(iprot, oprot, server_ctx)
  local name, mtype, seqid = iprot:readMessageBegin()
  local func_name = 'process_' .. name
  if not self[func_name] or ttype(self[func_name]) ~= 'function' then
    if oprot ~= nil then
      iprot:skip(TType.STRUCT)
      iprot:readMessageEnd()
      x = TApplicationException:new{
        errorCode = TApplicationException.UNKNOWN_METHOD
      }
      oprot:writeMessageBegin(name, TMessageType.EXCEPTION, seqid)
      x:write(oprot)
      oprot:writeMessageEnd()
      oprot.trans:flush()
    end
    return false, 'Unknown function '..name
  else
    return self[func_name](self, seqid, iprot, oprot, server_ctx)
  end
end

function MovieInfoServiceProcessor:process_GetMoviesByIds(seqid, iprot, oprot, server_ctx)
  local args = GetMoviesByIds_args:new{}
  local reply_type = TMessageType.REPLY
  args:read(iprot)
  iprot:readMessageEnd()
  local result = GetMoviesByIds_result:new{}
  local status, res = pcall(self.handler.GetMoviesByIds, self.handler, args.movie_ids)
  if not status then
    reply_type = TMessageType.EXCEPTION
    result = TApplicationException:new{message = res}
  else
    result.success = res
  end
  oprot:writeMessageBegin('GetMoviesByIds', reply_type, seqid)
  result:write(oprot)
  oprot:writeMessageEnd()
  oprot.trans:flush()
  return status, res
end

function MovieInfoServiceProcessor:process_GetMoviesByTitle(seqid, iprot, oprot, server_ctx)
  local args = GetMoviesByTitle_args:new{}
  local reply_type = TMessageType.REPLY
  args:read(iprot)
  iprot:readMessageEnd()
  local result = GetMoviesByTitle_result:new{}
  local status, res = pcall(self.handler.GetMoviesByTitle, self.handler, args.movie_string)
  if not status then
    reply_type = TMessageType.EXCEPTION
    result = TApplicationException:new{message = res}
  elseif ttype(res) == 'ServiceException' then
    result.se = res
  else
    result.success = res
  end
  oprot:writeMessageBegin('GetMoviesByTitle', reply_type, seqid)
  result:write(oprot)
  oprot:writeMessageEnd()
  oprot.trans:flush()
  return status, res
end

-- HELPER FUNCTIONS AND STRUCTURES

local GetMoviesByIds_args = __TObject:new{
  movie_ids
}

function GetMoviesByIds_args:read(iprot)
  iprot:readStructBegin()
  while true do
    local fname, ftype, fid = iprot:readFieldBegin()
    if ftype == TType.STOP then
      break
    elseif fid == 1 then
      if ftype == TType.LIST then
        self.movie_ids = {}
        local _etype15, _size12 = iprot:readListBegin()
        for _i=1,_size12 do
          local _elem16 = iprot:readString()
          table.insert(self.movie_ids, _elem16)
        end
        iprot:readListEnd()
      else
        iprot:skip(ftype)
      end
    else
      iprot:skip(ftype)
    end
    iprot:readFieldEnd()
  end
  iprot:readStructEnd()
end

function GetMoviesByIds_args:write(oprot)
  oprot:writeStructBegin('GetMoviesByIds_args')
  if self.movie_ids ~= nil then
    oprot:writeFieldBegin('movie_ids', TType.LIST, 1)
    oprot:writeListBegin(TType.STRING, #self.movie_ids)
    for _,iter17 in ipairs(self.movie_ids) do
      oprot:writeString(iter17)
    end
    oprot:writeListEnd()
    oprot:writeFieldEnd()
  end
  oprot:writeFieldStop()
  oprot:writeStructEnd()
end

 GetMoviesByIds_result = __TObject:new{
  success
}

function GetMoviesByIds_result:read(iprot)
  iprot:readStructBegin()
  while true do
    local fname, ftype, fid = iprot:readFieldBegin()
    if ftype == TType.STOP then
      break
    elseif fid == 0 then
      if ftype == TType.LIST then
        self.success = {}
        local _etype21, _size18 = iprot:readListBegin()
        for _i=1,_size18 do
          local _elem22 = iprot:readString()
          table.insert(self.success, _elem22)
        end
        iprot:readListEnd()
      else
        iprot:skip(ftype)
      end
    else
      iprot:skip(ftype)
    end
    iprot:readFieldEnd()
  end
  iprot:readStructEnd()
end

function GetMoviesByIds_result:write(oprot)
  oprot:writeStructBegin('GetMoviesByIds_result')
  if self.success ~= nil then
    oprot:writeFieldBegin('success', TType.LIST, 0)
    oprot:writeListBegin(TType.STRING, #self.success)
    for _,iter23 in ipairs(self.success) do
      oprot:writeString(iter23)
    end
    oprot:writeListEnd()
    oprot:writeFieldEnd()
  end
  oprot:writeFieldStop()
  oprot:writeStructEnd()
end

local GetMoviesByTitle_args = __TObject:new{
  movie_string
}

function GetMoviesByTitle_args:read(iprot)
  iprot:readStructBegin()
  while true do
    local fname, ftype, fid = iprot:readFieldBegin()
    if ftype == TType.STOP then
      break
    elseif fid == 1 then
      if ftype == TType.STRING then
        self.movie_string = iprot:readString()
      else
        iprot:skip(ftype)
      end
    else
      iprot:skip(ftype)
    end
    iprot:readFieldEnd()
  end
  iprot:readStructEnd()
end

function GetMoviesByTitle_args:write(oprot)
  oprot:writeStructBegin('GetMoviesByTitle_args')
  if self.movie_string ~= nil then
    oprot:writeFieldBegin('movie_string', TType.STRING, 1)
    oprot:writeString(self.movie_string)
    oprot:writeFieldEnd()
  end
  oprot:writeFieldStop()
  oprot:writeStructEnd()
end

 GetMoviesByTitle_result = __TObject:new{
  success,
  se
}

function GetMoviesByTitle_result:read(iprot)
  iprot:readStructBegin()
  while true do
    local fname, ftype, fid = iprot:readFieldBegin()
    if ftype == TType.STOP then
      break
    elseif fid == 0 then
      if ftype == TType.LIST then
        self.success = {}
        local _etype27, _size24 = iprot:readListBegin()
        for _i=1,_size24 do
          local _elem28 = iprot:readString()
          table.insert(self.success, _elem28)
        end
        iprot:readListEnd()
      else
        iprot:skip(ftype)
      end
    elseif fid == 1 then
      if ftype == TType.STRUCT then
        self.se = ServiceException:new{}
        self.se:read(iprot)
      else
        iprot:skip(ftype)
      end
    else
      iprot:skip(ftype)
    end
    iprot:readFieldEnd()
  end
  iprot:readStructEnd()
end

function GetMoviesByTitle_result:write(oprot)
  oprot:writeStructBegin('GetMoviesByTitle_result')
  if self.success ~= nil then
    oprot:writeFieldBegin('success', TType.LIST, 0)
    oprot:writeListBegin(TType.STRING, #self.success)
    for _,iter29 in ipairs(self.success) do
      oprot:writeString(iter29)
    end
    oprot:writeListEnd()
    oprot:writeFieldEnd()
  end
  if self.se ~= nil then
    oprot:writeFieldBegin('se', TType.STRUCT, 1)
    self.se:write(oprot)
    oprot:writeFieldEnd()
  end
  oprot:writeFieldStop()
  oprot:writeStructEnd()
end
