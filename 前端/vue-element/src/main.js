import Vue from 'vue'

import Cookies from 'js-cookie'

import 'normalize.css/normalize.css' // a modern alternative to CSS resets

import Element from 'element-ui'
import './styles/element-variables.scss'
import enLang from 'element-ui/lib/locale/lang/en'// 如果使用中文语言包请默认支持，无需额外引入，请删除该依赖

import '@/styles/index.scss' // global css

import App from './App'
import store from './store'
import router from './router'

import './icons' // icon
import './permission' // permission control
import './utils/error-log' // error log

import * as filters from './filters' // global filters

import VueAMap from 'vue-amap'



/**
 * If you don't want to use mock-server
 * you want to use MockJs for mock api
 * you can execute: mockXHR()
 *
 * Currently MockJs will be used in the production environment,
 * please remove it before going online ! ! !
 */
if (process.env.NODE_ENV === 'production') {
  const { mockXHR } = require('../mock')
  mockXHR()
}

Vue.use(Element, {
  size: Cookies.get('size') || 'medium', // set element-ui default size
  locale: enLang // 如果使用中文，无需设置，请删除
})

/**
* 高德地图
* */
Vue.use(VueAMap)
VueAMap.initAMapApiLoader({
  key: '9bd23a77b526d166f711d55a44ee58cd',
  plugin: ['AMap.Scale', 'AMap.OverView', 'AMap.ToolBar', 'AMap.MapType', 'AMap.PlaceSearch', 'AMap.Geolocation', 'AMap.Geocoder', 'AMap.DistrictSearch'],
  v: '1.4.4',
  uiVersion: '1.0'
})


/**
 * 按数字大小转换为 万 或者 亿 为单位的数字
 */
Vue.filter('NumToUnitNum', function (value) {
  if (!value) return '0.00'
  if (value > 100000000 || value < -100000000) {
    return Number(value/100000000).toFixed(2)
  }else if (value > 10000 || value < -10000) {
    return Number(value/10000).toFixed(2)
  }else {
    return Number(value).toFixed(2)
  }
})

/**
 * 通过数字获取到数字转换后的单位
 */
Vue.filter('GetUnit', function (value) {
  if (!value) " "
  value = Math.abs(value)
  if (value > 100000000) {
    return "亿"
  }else if (value > 10000) {
    return "万"
  }else {
    return "元"
  }
})


// register global utility filters
Object.keys(filters).forEach(key => {
  Vue.filter(key, filters[key])
})

Vue.config.productionTip = false

new Vue({
  el: '#app',
  router,
  store,
  render: h => h(App)
})

Vue.prototype.bus = new Vue()  // 创建全局的事件总线对象
