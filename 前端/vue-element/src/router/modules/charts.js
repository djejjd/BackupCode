/** When your routing table is too long, you can split it into small modules**/

import Layout from '@/layout'

const chartsRouter = {
  path: '/charts',
  component: Layout,
  redirect: 'noRedirect',
  name: 'Charts',
  meta: {
    title: '统计展示',
    icon: 'chart'
  },
  children: [
    {
      path: 'keyboard',
      component: () => import('@/views/charts/keyboard'),
      name: 'KeyboardChart',
      meta: { title: '药品使用情况', noCache: true }
    },
    {
      path: 'line',
      component: () => import('@/views/charts/line'),
      name: 'LineChart',
      meta: { title: '药品费用情况', noCache: true }
    },
    {
      path: 'mix-chart',
      component: () => import('@/views/charts/mix-chart'),
      name: 'MixChart',
      meta: { title: '年龄分布', noCache: true }
    },
    // {
    //   path: 'map-chart',
    //   component: () => import('@/views/charts/map-chart'),
    //   name: 'MapChart',
    //   meta: { title: '地理分布', noCache: true }
    // }
    {
      path: 'disease-chart',
      component: () => import('@/views/charts/disease-chart'),
      name: 'DiseaseChart',
      meta: { title: '病例统计', noCache: true }
    }
  ]
}

export default chartsRouter
