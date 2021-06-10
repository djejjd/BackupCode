import request from '@/utils/request'

export function getDrugChart(query) {
  return request({
    url: 'http://127.0.0.1:5000/dev-api/drug',
    method: 'get',
    params: query
  })
}


export function getDrugFeeChart(query) {
  return request({
    url: 'http://127.0.0.1:5000/dev-api/drugFee',
    method: 'get',
    params: query
  })
}

export function getDataAgeChart(query) {
  return request({
    url: 'http://127.0.0.1:5000/dev-api/age',
    method: 'get',
    params: query
  })
}

export function getDataMapChart(query) {
  return request({
    url: 'http://127.0.0.1:5000/dev-api/map',
    method: 'get',
    params: query
  })
}


export function getDataPredictChart(query) {
  return request({
    url: 'http://127.0.0.1:5000/dev-api/predict',
    method: 'get',
    params: query
  })
}

export function getDataDiseaseChart(query) {
  return request({
    url: 'http://127.0.0.1:5000/dev-api/disease',
    method: 'get',
    params: query
  })
}

export function getDataGrowthRateChart(query) {
  return request({
    url: 'http://127.0.0.1:5000/dev-api/growthRate',
    method: 'get',
    params: query
  })
}

export function getDataKMeansChart(query) {
  return request({
    url: 'http://127.0.0.1:5000/dev-api/kMeans',
    method: 'get',
    params: query
  })
}


export function getDataDiseaseCodeChart(query) {
  return request({
    url: 'http://127.0.0.1:5000/dev-api/diseaseCode',
    method: 'get',
    params: query
  })
}

export function getLoginInfo(query) {
  return request({
    url: 'http://127.0.0.1:5000/dev-api/login',
    method: 'get',
    params: query
  })
}

export function getRole(query) {
  return request({
    url: 'http://127.0.0.1:5000/dev-api/getRole',
    method: 'get',
    params: query
  })
}

export function addRole(query) {
  return request({
    url: 'http://127.0.0.1:5000/dev-api/addRole',
    method: 'get',
    params: query
  })
}

export function deleteRole(query) {
  return request({
    url: 'http://127.0.0.1:5000/dev-api/deleteRole',
    method: 'get',
    params: query
  })
}

export function updateRole(query) {
  return request({
    url: 'http://127.0.0.1:5000/dev-api/updateRole',
    method: 'get',
    params: query
  })
}


